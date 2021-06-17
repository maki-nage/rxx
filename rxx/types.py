from collections import namedtuple

NamedObservable = namedtuple('NamedObservable', ['name', 'id', 'observable'])
NamedObservable.__doc__ = "Item definition for a named higher-order observable"
NamedObservable.name.__doc__ = "The name of the observable"
NamedObservable.id.__doc__ = "The identifier of the observable"
NamedObservable.observable.__doc__ = "The observable object"

Update = namedtuple('Update', [])
Updated = namedtuple('Updated', [])
