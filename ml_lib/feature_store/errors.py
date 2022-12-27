from typing import Optional

from pydantic import ValidationError


def map_validation_error_field_type(type: Optional[str]) -> str:
    if type == "value_error.missing":
        return "missing"
    else:
        return "invalid"


class OfflineClientNotConfiguredError(Exception):
    validation_error: ValidationError

    def __init__(self, validation_error: ValidationError) -> None:
        super().__init__("Offline client not configured")
        self.validation_error = validation_error

    def __str__(self) -> str:
        return (
            super().__str__()
            + "\n"
            + "\n".join(
                [
                    f'\t{".".join(str(error.get("loc", "__root__")))} {map_validation_error_field_type(error.get("type"))}'
                    for error in self.validation_error.errors()
                ]
            )
        )


class OnlineClientNotConfiguredError(Exception):
    validation_error: ValidationError

    def __init__(self, validation_error: ValidationError) -> None:
        super().__init__("Online client not configured")
        self.validation_error = validation_error

    def __str__(self) -> str:
        return (
            super().__str__()
            + "\n"
            + "\n".join(
                [
                    f'\t{".".join(str(error.get("loc", "__root__")))} {map_validation_error_field_type(error.get("type"))}'
                    for error in self.validation_error.errors()
                ]
            )
        )


class ProjectScopeNotConfiguredError(Exception):
    validation_error: ValidationError

    def __init__(self, validation_error: ValidationError) -> None:
        super().__init__("Project scope not configured")
        self.validation_error = validation_error

    def __str__(self) -> str:
        return (
            super().__str__()
            + "\n"
            + "\n".join(
                [
                    f'\t{".".join(str(error.get("loc", "__root__")))} {map_validation_error_field_type(error.get("type"))}'
                    for error in self.validation_error.errors()
                ]
            )
        )


class EntryNotFoundError(Exception):
    entry_name: str

    def __init__(self, entry_name: str) -> None:
        super().__init__("Entry not found")
        self.entry_name = entry_name

    def __str__(self) -> str:
        return f"{super().__str__()} (name={self.entry_name})"
