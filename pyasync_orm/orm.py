class ORM:
    def __init__(self, model_class):
        self.model_class = model_class

    async def count(self):
        pass

    async def oder_by(self):
        pass

    async def create(self, **kwargs):
        pass

    async def bulk_create(self, **kwargs):
        pass

    async def get(self, **kwargs):
        pass

    async def all(self, **kwargs):
        pass

    async def filter(self, **kwargs):
        pass

    async def exclude(self, **kwargs):
        pass

    async def update(self, **kwargs):
        pass

    async def bulk_update(self, **kwargs):
        pass

    async def delete(self, **kwargs):
        pass
