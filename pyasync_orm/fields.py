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
