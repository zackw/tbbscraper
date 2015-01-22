from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    name='cld2',
    packages=['cld2'],
    package_dir={'cld2':''},
    ext_modules=[Extension("cld2", language="c++", sources=[
        "cld2.pyx",
        "cld2_generated_cjk_compatible.cc",
        "cld2_generated_deltaocta0122.cc",
        "cld2_generated_distinctocta0122.cc",
        "cld2_generated_quad0122.cc",
        "cld_generated_cjk_delta_bi_32.cc",
        "cld_generated_cjk_uni_prop_80.cc",
        "cld_generated_score_quad_octa_0122.cc",
        "cldutil.cc",
        "cldutil_shared.cc",
        "compact_lang_det_hint_code.cc",
        "compact_lang_det_impl.cc",
        "compact_lang_det.cc",
        "generated_distinct_bi_0.cc",
        "generated_language.cc",
        "generated_ulscript.cc",
        "getonescriptspan.cc",
        "lang_script.cc",
        "scoreonescriptspan.cc",
        "tote.cc",
        "utf8statetable.cc",
    ])],
    cmdclass = {'build_ext':build_ext}
)
