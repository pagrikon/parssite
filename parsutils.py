# Copyright: 2016, Pavel Konurkin
# Author: Pavel Konurkin
# License: BSD
"""
Utils for parsing sites
"""
from __future__ import print_function
import parssite as ps
import yaml


class Config(object):

    def __init__(self, fileName):
        self._attrNames = set()
        config = yaml.load(open(fileName))
        for key in config:
            setattr(self, key, config[key])
        self.use()

    def __iter__(self):
        return iter(self._attrNames)

    def __getitem__(self, index):
        return getattr(self, index)

    def use(self):
        ps.PageCache.cacheDir = self.cacheDir
        ps.Web.hammer_timeouts = self.hammer_timeouts
        ps.Web.randomDelayPeriod = self.randomDelayPeriod
        ps.Web.logDir = self.webLogDir
        ps.Web.proxyStatDir = self.proxyStatDir
        ps.Web.errorLogDir = self.webErrorLogDir
        ps.Web.log404dir = self.log404dir
        ps.WebPage.pageConfirmedDefault = self.pageConfirmedDefault
        ps.ParsBase._saveInstanceDefault = self.saveInstanceDefault
        ps.PageCache.memoryCache = self.memoryCache
        ps.Web.allow404 = self.allow404
        ps.XlsxSheet.disableFormulaDefault = self.disableFormulaExcel
        ps.Web.proxyFile = self.proxyFile
        ps.PageCache.onlyFromCache = self.onlyFromCache
        ps.ParsBase._NoneObjectUnicode = unicode(self.noneElemView)

    def __getattr__(self, name):
        if name == 'cacheDir':
            return ps.PageCache.cacheDir
        elif name == 'hammer_timeouts':
            return ps.Web.hammer_timeouts
        elif name == 'randomDelayPeriod':
            return ps.Web.randomDelayPeriod
        elif name == 'webLogDir':
            return ps.Web.logDir
        elif name == 'proxyStatDir':
            return ps.Web.proxyStatDir
        elif name == 'webErrorLogDir':
            return ps.Web.errorLogDir
        elif name == 'log404dir':
            return ps.Web.log404dir
        elif name == 'timeInPath':
            return True
        elif name == 'pageConfirmedDefault':
            return ps.WebPage.pageConfirmedDefault
        elif name == 'saveInstanceDefault':
            return ps.ParsBase._saveInstanceDefault
        elif name == 'memoryCache':
            return ps.PageCache.memoryCache
        elif name == 'allow404':
            return ps.Web.allow404
        elif name == 'disableFormulaExcel':
            return ps.XlsxSheet.disableFormulaDefault
        elif name == 'proxyFile':
            return ps.Web.proxyFile
        elif name == 'onlyFromCache':
            return ps.PageCache.onlyFromCache
        elif name == 'noneElemView':
            return ps.ParsBase._NoneObjectUnicode
        else:
            return None

    def __setattr__(self, name, value):
        if name[0] != '_':
            self._attrNames.add(name)
        if name == 'hammer_timeouts':
            if value is not None:
                value = tuple([tuple(x) for x in value])
        elif name == 'randomDelayPeriod':
            if value is not None:
                value = tuple(value)
        elif name in ('cacheDir', 'webLogDir', 'proxyStatDir'
                      , 'webErrorLogDir', 'log404dir', 'outDir'):
            value = ps.normalizePath(value, itDir=True)
        object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name == 'outDir':
            value = object.__getattribute__(self, name)
            if self.timeInPath:
                value += ps.createDateFileName() + '/'
            return value
        else:
            return object.__getattribute__(self, name)

    def print(self):
        for name in sorted(self._attrNames):
            value = self[name]
            if type(value) is unicode:
                value = value.encode(ps.ParsBase._encoding)
            print(name, ': ', value, sep='')
