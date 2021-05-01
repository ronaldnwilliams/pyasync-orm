class BaseField:
    pass


class Serial(BaseField):
    def __init__(self, primary_key: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.primary_key = primary_key


class SmallInt(Serial):
    pass


class Integer(Serial):
    pass


class BigInt(Serial):
    pass
