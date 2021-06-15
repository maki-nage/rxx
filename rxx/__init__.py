__author__ = """Romain Picard"""
__email__ = 'romain.picard@oakbits.com'
__version__ = '0.0.0'

from collections import namedtuple

NamedObservable = namedtuple('NamedObservable', ['name', 'id', 'observable'])
NamedObservable.__doc__ = "Item definition for a named higher-order observable"
NamedObservable.name.__doc__ = "The name of the observable"
NamedObservable.id.__doc__ = "The identifier of the observable"
NamedObservable.observable.__doc__ = "The observable object"

from . import feedback
