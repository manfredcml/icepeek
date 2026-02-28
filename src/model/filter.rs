use anyhow::{bail, Result};
use iceberg::expr::{Predicate, Reference};

/// Parse a simplified filter expression into an iceberg Predicate.
///
/// Supported syntax:
/// - `column > value`, `column < value`, `column >= value`, `column <= value`
/// - `column = value`, `column != value`
/// - `column = 'string value'`
/// - `column IS NULL`, `column IS NOT NULL`
/// - `column IN ('a', 'b', 'c')`
/// - Combinators: `expr AND expr`, `expr OR expr`
///
/// Values without quotes are parsed as numbers; quoted values as strings.
pub fn parse_filter(input: &str) -> Result<Predicate> {
    let input = input.trim();
    if input.is_empty() {
        bail!("empty filter expression");
    }
    parse_or_expr(input)
}

fn parse_or_expr(input: &str) -> Result<Predicate> {
    let Some((left, right)) = split_combinator(input, " OR ") else {
        return parse_and_expr(input);
    };
    let l = parse_and_expr(left)?;
    let r = parse_or_expr(right)?;
    Ok(l.or(r))
}

fn parse_and_expr(input: &str) -> Result<Predicate> {
    let Some((left, right)) = split_combinator(input, " AND ") else {
        return parse_comparison(input);
    };
    let l = parse_comparison(left)?;
    let r = parse_and_expr(right)?;
    Ok(l.and(r))
}

/// Split on first occurrence of combinator, respecting quoted strings.
fn split_combinator<'a>(input: &'a str, combinator: &str) -> Option<(&'a str, &'a str)> {
    let upper = input.to_uppercase();
    let mut in_quote = false;

    for (i, c) in input.char_indices() {
        if c == '\'' {
            in_quote = !in_quote;
        }
        if !in_quote && upper[i..].starts_with(combinator) {
            let left = &input[..i];
            let right = &input[i + combinator.len()..];
            return Some((left.trim(), right.trim()));
        }
    }
    None
}

fn parse_comparison(input: &str) -> Result<Predicate> {
    let input = input.trim();
    let upper = input.to_uppercase();

    // IS NOT NULL
    if upper.ends_with(" IS NOT NULL") {
        let col = input[..input.len() - 12].trim();
        return Ok(Reference::new(col).is_not_null());
    }

    // IS NULL
    if upper.ends_with(" IS NULL") {
        let col = input[..input.len() - 8].trim();
        return Ok(Reference::new(col).is_null());
    }

    // IN ('a', 'b', ...)
    if let Some(in_pos) = find_keyword_pos(&upper, " IN ") {
        let col = input[..in_pos].trim();
        let list_part = input[in_pos + 4..].trim();
        if list_part.starts_with('(') && list_part.ends_with(')') {
            let inner = &list_part[1..list_part.len() - 1];
            let values = parse_list_values(inner)?;
            let datum_values: Vec<iceberg::spec::Datum> =
                values.into_iter().map(|v| string_to_datum(&v)).collect();
            return Ok(Reference::new(col).is_in(datum_values));
        }
        bail!("invalid IN expression: {}", input);
    }

    // Comparison operators: >=, <=, !=, >, <, =
    let operators = [">=", "<=", "!=", ">", "<", "="];
    for op in &operators {
        let Some(pos) = input.find(op) else { continue };

        let col = input[..pos].trim();
        let val_str = input[pos + op.len()..].trim();
        let datum = string_to_datum(val_str);

        return match *op {
            ">=" => Ok(Reference::new(col).greater_than_or_equal_to(datum)),
            "<=" => Ok(Reference::new(col).less_than_or_equal_to(datum)),
            "!=" => Ok(Reference::new(col).not_equal_to(datum)),
            ">" => Ok(Reference::new(col).greater_than(datum)),
            "<" => Ok(Reference::new(col).less_than(datum)),
            "=" => Ok(Reference::new(col).equal_to(datum)),
            _ => unreachable!(),
        };
    }

    bail!("cannot parse filter expression: {}", input);
}

fn find_keyword_pos(upper: &str, keyword: &str) -> Option<usize> {
    let mut in_quote = false;
    for (i, c) in upper.char_indices() {
        if c == '\'' {
            in_quote = !in_quote;
        }
        if !in_quote && upper[i..].starts_with(keyword) {
            return Some(i);
        }
    }
    None
}

fn parse_list_values(input: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for c in input.chars() {
        match c {
            '\'' if in_quote => {
                in_quote = false;
            }
            '\'' => {
                in_quote = true;
            }
            ',' if !in_quote => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    values.push(trimmed);
                }
                current.clear();
            }
            _ => {
                if in_quote || !c.is_whitespace() {
                    current.push(c);
                }
            }
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        values.push(trimmed);
    }
    Ok(values)
}

/// Convert a string value to an iceberg Datum.
/// Quoted strings become string datums; unquoted numeric values become appropriate types.
fn string_to_datum(val: &str) -> iceberg::spec::Datum {
    let val = val.trim();

    // Quoted string
    if val.starts_with('\'') && val.ends_with('\'') && val.len() >= 2 {
        let inner = &val[1..val.len() - 1];
        return iceberg::spec::Datum::string(inner);
    }

    // Try integer
    if let Ok(i) = val.parse::<i64>() {
        return iceberg::spec::Datum::long(i);
    }

    // Try float
    if let Ok(f) = val.parse::<f64>() {
        return iceberg::spec::Datum::double(f);
    }

    // Boolean
    match val.to_lowercase().as_str() {
        "true" => return iceberg::spec::Datum::bool(true),
        "false" => return iceberg::spec::Datum::bool(false),
        _ => {}
    }

    // Fallback: treat as string
    iceberg::spec::Datum::string(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_greater_than() {
        let result = parse_filter("price > 100");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_string_equality() {
        let result = parse_filter("category = 'electronics'");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_is_null() {
        let result = parse_filter("name IS NULL");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_is_not_null() {
        let result = parse_filter("name IS NOT NULL");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_and_combinator() {
        let result = parse_filter("price > 100 AND category = 'electronics'");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_or_combinator() {
        let result = parse_filter("price > 100 OR price < 10");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_in_list() {
        let result = parse_filter("status IN ('active', 'pending')");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_empty_filter_fails() {
        assert!(parse_filter("").is_err());
        assert!(parse_filter("   ").is_err());
    }

    #[test]
    fn parse_invalid_filter_fails() {
        assert!(parse_filter("nonsense gibberish").is_err());
    }

    #[test]
    fn string_to_datum_types() {
        // Integer
        let d = string_to_datum("42");
        assert_eq!(
            format!("{:?}", d),
            format!("{:?}", iceberg::spec::Datum::long(42))
        );

        // String
        let d = string_to_datum("'hello'");
        assert_eq!(
            format!("{:?}", d),
            format!("{:?}", iceberg::spec::Datum::string("hello"))
        );
    }
}
