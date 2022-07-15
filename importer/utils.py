def error_handler(exc):
    return str(exc.detail if hasattr(exc, "detail") else exc.args[0])