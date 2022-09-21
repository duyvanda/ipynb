#!/usr/bin/env python3

from .module import suml
from ..moduleA import suml as suml2


def print_sump():
    print(suml([1,2]))


def print_sump2():
    print(suml2([2,2]))