from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

setup(name='html_extractor',
      packages=['html_extractor'],
      package_dir={'html_extractor':''},
      ext_modules=cythonize([
          Extension("relative_urls", ["relative_urls.pyx"]),
          Extension("_extractor", ["_extractor.pyx"],
                    libraries=["gumbo"])
      ])
)
