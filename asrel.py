#!/usr/bin/env python

'''AS relationships

This module loads CAIDA's AS relationship database and provides easy
access functions.  CAIDA's database contains provider-customers and
peer-peer links (but no sibling links).  At the moment of this
writing, you can check CAIDA's AS relationship dataset at [1].

[1] http://data.caida.org/datasets/2013-asrank-data-supplement/'''

import re
import gzip
from collections import namedtuple

P2C = -1
P2P = 0
C2P = 1

rel2str = {P2C: 'P2C', P2P: 'P2P', C2P: 'C2P'}

class ASRelationshipsDB(object):#{{{
    def __init__(self, fn):#{{{
        self.sources = set()
        self.clique = set()
        self.ixps = set()
        self.pair2rel = dict()

        fd = gzip.open(fn, 'r')
        for line in fd:
            line = line.strip()
            if self._parse_source(line):
                continue
            if self._parse_clique(line):
                continue
            if self._parse_ixps(line):
                continue
            if self._parse_relationship(line):
                continue
            assert line[0] == '#'
        fd.close()

        assert self.sources
        assert self.clique
        assert self.ixps
        assert len(self.pair2rel) > 1000
    #}}}

    def _parse_source(self, line):#{{{
        m = re.match(r'^# source:(.*)$', line)
        if m is None: return False
        src = DataSource(*m.group(1).split('|'))
        assert src.dtype == 'topology'
        assert src.proto == 'BGP'
        assert int(src.date) > 0
        self.sources.add(src)
        return True
    #}}}

    def _parse_clique(self, line):#{{{
        m = re.match(r'^# inferred clique: ([\d\s]+)$', line)
        if m is None: return False
        self.clique = set(int(asn) for asn in m.group(1).split())
        return True
    #}}}

    def _parse_ixps(self, line):#{{{
        m = re.match(r'^# IXP ASes: ([\d\s]+)$', line)
        if m is None: return False
        self.ixps = set(int(asn) for asn in m.group(1).split())
        return True
    #}}}

    def _parse_relationship(self, line):#{{{
        m = _relationship_regexp.match(line)
        if m is None: return False
        as1, as2, rel = tuple(int(i) for i in m.groups())
        assert rel in _relationship_valid
        self.pair2rel[(as1, as2)] = rel
        return True
    #}}}

    def __getitem__(self, pair):#{{{
        if pair in self.pair2rel:
            return self.pair2rel[pair]
        pair = pair[1], pair[0]
        if pair in self.pair2rel:
            return -1 * self.pair2rel[pair]
        raise KeyError('unknown relationship %d-%d' % (pair[1], pair[0]))
    #}}}

    def get(self, pair, missing=None):#{{{
        try:
            return self[pair]
        except KeyError:
            return missing
    #}}}
#}}}


# Parsed from lines starting with '# source:'
DataSource = namedtuple('DataSource',
        ['dtype', 'proto', 'date', 'feed', 'collector'])


# Format is as1|as2|rel; where -1 means P2C and 0 means P2P
_relationship_regexp = re.compile(r'^(\d+)\|(\d+)\|([0-9-]+)$')
_relationship_valid = set([P2C, P2P])


if __name__ == '__main__':
    db = ASRelationshipsDB('20130801.as-rel.txt.gz')
    assert db[1, 2] == -1
    assert db[2, 1] == 1
    assert db[1, 1614] == 0
    assert db[1614, 1] == 0
