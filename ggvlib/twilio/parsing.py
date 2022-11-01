def to_form_field(field_name: str) -> str:
    return field_name.replace("_", " ").title().replace(" ", "")
