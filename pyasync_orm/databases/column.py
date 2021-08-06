from typing import Any, Optional, Union, Callable


class Column:
    def __init__(
        self,
        column_name: str,
        data_type: Any,
        null: bool,
        unique: bool,
        default: Optional[Union[Any, Callable[[Any], Any]]] = None,
        max_length: Optional[int] = None,
        max_digits: Optional[int] = None,
        decimal_places: Optional[int] = None,
    ):
        self.column_name = column_name
        self.data_type = data_type
        self.null = null
        self.unique = unique
        self.default = default
        self.max_length = max_length
        self.max_digits = max_digits
        self.decimal_places = decimal_places
