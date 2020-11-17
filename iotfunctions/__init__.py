import os
import pkgutil

__version__ = '8.2.1'
__all__ = list(module for (_, module, _) in pkgutil.iter_modules([os.path.dirname(__file__)]))
