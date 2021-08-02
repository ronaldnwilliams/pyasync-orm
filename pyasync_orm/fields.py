class BaseField:
    def __init__(
        self,
        primary_key: bool = False,
    ):
        self.primary_key = primary_key


class SmallSerial(BaseField):
    pass


class Serial(BaseField):
    pass


class BigSerial(BaseField):
    pass


class VarChar(BaseField):
    def __init__(self, max_length: int, **kwargs):
        self.max_length = max_length
        super().__init__(**kwargs)
