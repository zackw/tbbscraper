#! /usr/bin/python3

# Generates a compressed lookup table and some ancillary data from the
# Unicode property file GraphemeBreakProperty.txt.

from collections import defaultdict
import os
import sys
import textwrap

CLASSES = {
    'Other'              : 0,
    'CR'                 : 1,
    'LF'                 : 2,
    'Control'            : 3,
    'Extend'             : 4,
    'SpacingMark'        : 5,
    'L'                  : 6,
    'V'                  : 7,
    'T'                  : 8,
    'LV'                 : 9,
    'LVT'                : 10,
    'Regional_Indicator' : 11
}

rCLASSES = sorted(CLASSES.items(), key = lambda kv: kv[1])

# Shorthand notation used in the main table.
CLASSCODES = {
    0:   '_',
    1:  'CR',
    2:  'LF',
    3:   'C',
    4:   'X',
    5:   'S',
    6:   'L',
    7:   'V',
    8:   'T',
    9:  'LV',
    10:  'H',
    11: 'RI',
}

# CLUSTER_BOUNDARY[a][b] is true if and only if there is a (default,
# extended) cluster boundary between a character of class A and a
# character of class B, *in that order*.

# http://unicode.org/reports/tr29/#Grapheme_Cluster_Boundary_Rules
# GB1 and GB2 are handled in the caller

MAX_CLASS = max(CLASSES.values()) + 1

# GB10, GB4, GB5
CLUSTER_BOUNDARY = [ [True] * MAX_CLASS for _ in range(MAX_CLASS) ]

# GB3
CLUSTER_BOUNDARY[CLASSES['CR']][CLASSES['LF']] = False

# GB6
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['L']] = False
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['V']] = False
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['LV']] = False
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['LVT']] = False

# GB7
CLUSTER_BOUNDARY[CLASSES['LV']][CLASSES['V']] = False
CLUSTER_BOUNDARY[CLASSES['LV']][CLASSES['T']] = False
CLUSTER_BOUNDARY[CLASSES['V']][CLASSES['V']] = False
CLUSTER_BOUNDARY[CLASSES['V']][CLASSES['T']] = False

# GB8
CLUSTER_BOUNDARY[CLASSES['LVT']][CLASSES['T']] = False
CLUSTER_BOUNDARY[CLASSES['T']][CLASSES['T']] = False

# GB8a
CLUSTER_BOUNDARY[CLASSES['Regional_Indicator']] \
                [CLASSES['Regional_Indicator']] = False

# GB9
CLUSTER_BOUNDARY[CLASSES['Other']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['Extend']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['SpacingMark']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['V']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['T']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['LV']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['LVT']][CLASSES['Extend']] = False
CLUSTER_BOUNDARY[CLASSES['Regional_Indicator']][CLASSES['Extend']] = False

# GB9a
CLUSTER_BOUNDARY[CLASSES['Other']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['Extend']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['SpacingMark']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['L']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['V']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['T']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['LV']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['LVT']][CLASSES['SpacingMark']] = False
CLUSTER_BOUNDARY[CLASSES['Regional_Indicator']][CLASSES['SpacingMark']] = False

# GB9b applies to no characters

def emit_cluster_boundary_table(f):
    f.write(
      "const uint8_t GBP_CLUSTER_BOUNDARY[GBP_MAX_CLASS][GBP_MAX_CLASS] = {\n")

    f.write("//")
    for name, value in rCLASSES:
        f.write(format(name[:3], ">4"))
    f.write("\n")

    for i, row in enumerate(CLUSTER_BOUNDARY):
        f.write(" { ")
        for col in row:
            f.write(" {:>2},".format(1 if col else 0))
        f.write("}, // " + rCLASSES[i][0] + "\n")

    f.write("};\n\n")

#
# Main property table
#

def read_header(tblf):
    # We just want the first line in the file, which identifies the
    # version of the Unicode character database that we're working from.
    header = tblf.readline()
    return header[1:].strip()

def read_table(tblf):
    full_table = [CLASSES['Other']] * 0x10FFFF

    for line in tblf:
        line = line.partition('#')[0].strip()
        if not line: continue
        codepoints, semi, cls = line.split()

        start, _, stop = codepoints.partition('..')
        start = int(start, 16)
        if stop:
            stop = int(stop, 16)
        else:
            stop = start

        assert semi == ';'

        cls = CLASSES[cls]

        for point in range(start, stop+1):
            full_table[point] = cls

    return full_table

def emit_table_annot(tag, n, indices, f):
    if tag == "plane":
        indices = ", ".join("{:06X}-{:06X}".format(c, c + 0xFFFF)
                            for c in indices)
    else:
        indices = ", ".join("{:06X}-{:06X}".format(c, c + 0xFF)
                            for c in indices)

    leader = "  // Non-uniform {} {}: ".format(tag, n)
    subseq = "  //" + " " * (len(leader) - 4)

    f.write(textwrap.fill(indices, 78,
                          initial_indent=leader,
                          subsequent_indent=subseq))
    f.write("\n")

def emit_table_block(table, f):
    if isinstance(table, str):
        table = [ord(c) for c in table]

    for i in range(0, len(table), 16):
        chunk = "".join("{:>3},".format(CLASSCODES[c & 0x7F]
                                        if c & 0x80
                                        else c)
                        for c in table[i:(i+16)])
        f.write("  ")
        f.write(chunk)
        f.write("\n")
    f.write("\n")

def emit_compressed_property_table(full_table, outf):
    # We generate a three-level lookup table.  The full Unicode range
    # is divided into planes; each plane is divided into 256-character
    # pages.  At both levels of division, if all characters have
    # exactly the same assignment, we just code that.  Also, all
    # subtables exhibiting exactly the same pattern are deduplicated;
    # this happens in particular for precomposed Hangul.
    #
    # The entire thing is encoded into one giant array of uint8_t.
    # The first 32 entries in this array are indexed by plane number
    # (with entries 17 through 31 unused).  After that come however
    # many 256-entry blocks are necessary to represent the complete
    # table.  All values follow this pattern: if the high bit of an
    # entry is set, the other seven bits are a literal class code (as
    # assigned above); otherwise they are a block number, i.e. val *
    # 256 + 32 is the zero index of the next block to reference.
    #
    # All the per-plane blocks are sorted ahead of all the per-page
    # blocks; within each group, blocks are sorted by the lowest
    # codepoint that uses that block.
    master_index = [CLASSES['Control']|0x80]*32
    plane_tables = {}
    plane_table_indices = {}
    plane_number = 0
    leaf_tables = {}
    leaf_table_indices = {}
    leaf_number = 0

    for pl_idx in range(0, 17):
        pl_lo = pl_idx * 0x10000
        pl_hi = pl_lo  + 0x10000
        plane = full_table[pl_lo:pl_hi]
        if all(p == plane[0] for p in plane):
            master_index[pl_idx] = 0x80 | plane[0]
        else:
            plane_table = [None]*256

            for pg_idx in range(0, 0x0100):
                pg_lo = pl_lo + pg_idx * 0x0100
                pg_hi = pg_lo + 0x0100
                page = full_table[pg_lo:pg_hi]
                if all(p == page[0] for p in page):
                    plane_table[pg_idx] = 0x80 | page[0]
                else:
                    leaf_table = "".join(chr(0x80 | c) for c in page)
                    if leaf_table in leaf_tables:
                        plane_table[pg_idx] = leaf_tables[leaf_table]
                        leaf_table_indices[leaf_table].append(pg_lo)
                    else:
                        leaf_table_indices[leaf_table] = [pg_lo]
                        leaf_tables[leaf_table] = leaf_number
                        plane_table[pg_idx] = leaf_number
                        leaf_number += 1

            plane_table = "".join(chr(c) for c in plane_table)
            if plane_table in plane_tables:
                master_index[pl_idx] = plane_tables[plane_table]
                plane_table_indices[plane_table].append(pl_lo)
            else:
                plane_table_indices[plane_table] = [pl_lo]
                plane_tables[plane_table] = plane_number
                master_index[pl_idx] = plane_number
                plane_number += 1

    plane_tables = sorted(plane_tables.keys(),
                          key = lambda x: plane_tables[x])
    leaf_tables  = sorted(leaf_tables.keys(),
                          key = lambda x: leaf_tables[x])

    # All the plane tables are emitted before all the leaf tables, so
    # we have to rewrite the leaf-table offsets (in the plane tables)
    # to skip over the plane tables.
    assert plane_number + leaf_number < 128
    for i in range(plane_number):
        original = plane_tables[i]
        adjusted = [ord(c) for c in original]
        adjusted = "".join(chr(c + plane_number if (c & 0x80) == 0 else c)
                           for c in adjusted)
        plane_table_indices[adjusted] = plane_table_indices[original]
        del plane_table_indices[original]
        plane_tables[i] = adjusted

    # Shorthand notation for the main table.
    undefs = []
    defines = []
    for name, value in rCLASSES:
        ccode = CLASSCODES[value]
        undefs.append("#undef {}\n".format(ccode))
        defines.append("#define {:<2} (0x80|GBP_{})\n".format(ccode, name))

    outf.writelines(undefs)
    outf.write("\n")
    outf.writelines(defines)
    outf.write("\n")

    outf.write("const uint8_t GBP_TABLE[] = {\n"
               "  // Top-level directory, indexed by plane number.\n"
               "  // (Entries 17 through 31 are padding.)\n")
    emit_table_block(master_index, outf)

    for n, tbl in enumerate(plane_tables):
        emit_table_annot("plane", n, plane_table_indices[tbl], outf)
        emit_table_block(tbl, outf)

    for n, tbl in enumerate(leaf_tables):
        emit_table_annot("leaf", n, leaf_table_indices[tbl], outf)
        emit_table_block(tbl, outf)

    outf.write("};\n")

def emit_file_prelude(f, header, cl):
    f.write("{} Generated by {} from {}.\n{} DO NOT EDIT.\n\n"
            .format(cl, os.path.basename(__file__), header, cl))

def emit_c_header(f, header):
    emit_file_prelude(f, header, "//")

    f.write("#ifndef gbp_tbl_h__\n"
            "#define gbp_tbl_h__\n"
            "\n"
            "#include <stdint.h>\n"
            "\n"
            "typedef enum GBP_Class {\n")

    for name, value in rCLASSES:
        f.write("  GBP_{} = {},\n".format(name, value))

    f.write("  GBP_MAX_CLASS = {}\n".format(MAX_CLASS))
    f.write("} GBP_Class;\n"
            "\n"
            "extern const uint8_t GBP_TABLE[];\n"
            "extern const uint8_t GBP_CLUSTER_BOUNDARY"
                "[GBP_MAX_CLASS][GBP_MAX_CLASS];"
            "\n"
            "static inline GBP_Class GBP_GetClass(uint32_t cpoint)\n"
            "{\n"
            "  if (cpoint > 0x10FFFF) return GBP_Other;\n"
            "\n"
            "  uint32_t plane = (cpoint & 0x1F0000) >> 16;\n"
            "  uint32_t page  = (cpoint & 0x00FF00) >>  8;\n"
            "  uint32_t point = (cpoint & 0x0000FF) >>  0;\n"
            "\n"
            "  uint32_t idx = GBP_TABLE[plane];\n"
            "  if (idx >= 0x80) return (GBP_Class)(idx & 0x7F);\n"
            "\n"
            "  idx = GBP_TABLE[idx*256 + 32 + page];\n"
            "  if (idx >= 0x80) return (GBP_Class)(idx & 0x7F);\n"
            "\n"
            "  idx = GBP_TABLE[idx*256 + 32 + point];\n"
            "  return (GBP_Class)(idx & 0x7F);\n"
            "}\n"
            "\n"
            "#endif // gbp_tbl_h__\n")

def emit_cython_glue(f, header):
    emit_file_prelude(f, header, "#")

    f.write("cdef extern from \"gbp_tbl.h\":\n"
            "\n"
            "    ctypedef enum GBP_Class:\n")

    for name, value in rCLASSES:
        f.write("        GBP_{}\n".format(name))

    f.write("\n"
            "    # Per Cython documentation, do not have to match the\n"
            "    # types exactly.\n"
            "    GBP_Class  GBP_GetClass(unsigned int cpoint)\n"
            "    const bint **GBP_CLUSTER_BOUNDARY")

def emit_c_source(f, header, table):
    emit_file_prelude(f, header, "//")

    f.write("#include \"gbp_tbl.h\"\n\n")

    emit_cluster_boundary_table(f)
    emit_compressed_property_table(table, f)

def main():
    with open("GraphemeBreakProperty.txt") as f:
        header = read_header(f)
        table  = read_table(f)

    with open("gbp_tbl.h", "wt") as f:
        emit_c_header(f, header)

    with open("gbp_tbl.pxd", "wt") as f:
        emit_cython_glue(f, header)

    with open("gbp_tbl.c", "wt") as f:
        emit_c_source(f, header, table)

main()
