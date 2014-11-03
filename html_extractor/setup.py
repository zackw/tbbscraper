from distutils.core import setup, Extension

mod = Extension('_html_extractor',
                sources = ['_html_extractor.c'])

setup (name = 'dummy',
       version = '1.0',
       ext_modules = [mod])
