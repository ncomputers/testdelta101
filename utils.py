import datetime

def timestamp_to_str(timestamp, fmt="%Y-%m-%d %H:%M:%S"):
    if timestamp > 1e12:
        dt = datetime.datetime.fromtimestamp(timestamp / 1e6)
    elif timestamp > 1e9:
        dt = datetime.datetime.fromtimestamp(timestamp / 1e3)
    else:
        dt = datetime.datetime.fromtimestamp(timestamp)
    return dt.strftime(fmt)

def round_decimal(value, precision=2):
    try:
        return round(float(value), precision)
    except Exception:
        return value

if __name__ == "__main__":
    test_timestamp_micro = 1742402453659000
    print("Microseconds timestamp:", test_timestamp_micro, "->", timestamp_to_str(test_timestamp_micro))
    test_timestamp_milli = 1742402453659
    print("Milliseconds timestamp:", test_timestamp_milli, "->", timestamp_to_str(test_timestamp_milli))
    test_value = "123.456789"
    print("Rounded value:", round_decimal(test_value, 2))
