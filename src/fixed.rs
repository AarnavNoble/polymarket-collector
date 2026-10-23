//! Decimal string to fixed-point conversion. Exchanges send prices and sizes
//! as decimal strings; parsing them through `f64` would silently lose
//! precision, so we parse digits straight into the output scale.

/// Output scale shared by every price and quantity: 1_000_000 = 1 unit.
pub const SCALE_DECIMALS: u32 = 6;

/// Parses a decimal string such as `"0.485"`, `"-12"` or `"1500.25"` into an
/// `i64` scaled by `10^6`. Rejects anything with more than six fractional
/// digits instead of rounding, so precision loss is always an error.
pub fn parse(s: &str) -> Option<i64> {
    let (neg, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if int.is_empty() && frac.is_empty() || frac.len() > SCALE_DECIMALS as usize {
        return None;
    }

    let mut value: i64 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
    }
    value = value.checked_mul(10i64.pow(SCALE_DECIMALS - frac.len() as u32))?;
    Some(if neg { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_forms() {
        assert_eq!(parse("0.485"), Some(485_000));
        assert_eq!(parse("1"), Some(1_000_000));
        assert_eq!(parse(".5"), Some(500_000));
        assert_eq!(parse("1500.25"), Some(1_500_250_000));
        assert_eq!(parse("-3.10"), Some(-3_100_000));
        assert_eq!(parse("0.000001"), Some(1));
    }

    #[test]
    fn rejects_bad_input() {
        for s in [
            "",
            "-",
            ".",
            "1.0000001",
            "1e5",
            "0x10",
            "1.2.3",
            " 1",
            "99999999999999",
        ] {
            assert_eq!(parse(s), None, "{s:?}");
        }
    }

    proptest::proptest! {
        #[test]
        fn roundtrips_through_formatting(v in -1_000_000_000_000i64..1_000_000_000_000) {
            let s = format!("{}{}.{:06}", if v < 0 { "-" } else { "" }, v.abs() / 1_000_000, v.abs() % 1_000_000);
            proptest::prop_assert_eq!(parse(&s), Some(v));
        }
    }
}
