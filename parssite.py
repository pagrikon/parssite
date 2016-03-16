# -*- coding: utf-8 -*-
# Copyright: 2016, Pavel Konurkin
# Author: Pavel Konurkin
# License: BSD
"""
Library for parsing sites
"""
from __future__ import print_function
from grab import Grab
from lxml import etree
import unicodedata
import re
from enum import Enum
import urlparse
import urllib
import copy
import pickle
import os
import os.path
import time
import random
import grab.error
import datetime
import sys


class ParsException(Exception):

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        self._attrSet = set()
        for attr in kwargs:
            self[attr] = kwargs[attr]

    def __getattr__(self, name):
        return None

    def __iter__(self):
        return iter(self._attrSet)

    def __getitem__(self, index):
        return getattr(self, index)

    def __setitem__(self, index, value):
        setattr(self, index, value)
        self._attrSet.add(index)

    def __str__(self):
        result = Exception.__str__(self)
        if len(result) > 0:
            result = '{' + result + '}'
        exceptionNames = set()
        for attrName in self:
            value = self[attrName]
            if isinstance(value, Exception):
                exceptionNames.add(attrName)
            else:
                result += '{' + attrName + ': ' + str(value) + '}'
        for attrName in exceptionNames:
            value = self[attrName]
            result += '{Exception ' + className(value) + ': ' + str(value) + '}'
        result = delElements(result, '\r\n')
        result = normalizeSpace(result)
        return result


class ParsError(ParsException):
    pass

    # def __init__(self, *args, **kwargs):
        # self.errorObj = kwargs.pop('errorObj', None)
        # Exception.__init__(self, *args, **kwargs)


class StructureError(ParsError):
    pass


class DirectionError(ParsError):
    pass


class DuplicateChildName(ParsError):
    pass


class UndefinedInstance(ParsError):
    pass


class UndefinedParent(ParsError):
    pass


class UndefinedQuery(ParsError):
    pass


class BadQueryResult(ParsError):
    pass

    # def __init__(self, *args, **kwargs):
        # self.queryResult = kwargs.pop('queryResult', None)
        # ParsError.__init__(self, *args, **kwargs)


class SetChildError(ParsError):
    pass


class UndefinedChildName(ParsError):
    pass


class UndefinedQueryRootName(ParsError):
    pass


class WebError(ParsError):
    pass


class WebInternalError(WebError):
    pass


class PageControlFault(WebError):
    pass


class WebServerError(WebError):
    pass


class WebClientError(WebError):
    pass


class ProxyError(WebError):
    pass


class ProxyServerError(ProxyError):
    pass


class AllProxyAlreadyUsed(ProxyError):
    pass


class PageCacheError(ParsError):
    pass


class ParsBreak(ParsException):
    pass


class PageError(ParsError):
    pass


class _Breaker():

    def __init__(self):
        self.iterNumber = 0

    def __call__(self, maxIterNumber):
        self.iterNumber += 1
        if self.iterNumber > maxIterNumber:
            raise ParsBreak


_breaker = _Breaker()

def className(instance, withPath=True):
    name = str(instance.__class__)
    if name[0] == '<':
        name = name[8:-2]
    if not withPath:
        name = name.rsplit('.', 1)[1]
    return name

def isParsStructure(obj):
    if isinstance(obj, ParsBase):
        return True
    elif type(obj) is list:
        for elem in obj:
            if not isParsStructure(elem):
                return False
    elif type(obj) is dict:
        for key in obj:
            if not isParsStructure(obj[key]):
                return False
    else:
        return False
    return True

def normalizeUrl(url):
    resultUrl = url
    resultUrl = urllib.unquote(resultUrl)
    resultUrl = resultUrl.strip()
    return resultUrl

def normalizeSpace(string):
    if type(string) is unicode:
        result = u''
    else:
        result = ''
    firstSpace = True
    for ch in string:
        if ch.isspace():
            if firstSpace:
                firstSpace = False
                result += ' '
        else:
            firstSpace = True
            result += ch
    result = result.strip()
    return result


class Direction(Enum):
    forward = 1
    backward = 2


class Structure(Enum):
    single = 1
    list = 2
    dict = 3


def printUnicodeInfoByChr(str):
    for ch in str:
        info = "ch='" + ch.encode(ParsBase._encoding) + "' " + \
            "category='" + unicodedata.category(ch) + "' " + \
            "escape='" + ch.encode('unicode_escape') + "' " + \
            "name='" + unicodedata.name(ch, 'UNDEFINE_NAME') + "'"
        print(info)


def delElements(sequence, delSequence):
    result = sequence[0: 0]
    for elem in sequence:
        insert = True
        for delElem in delSequence:
            if elem == delElem:
                insert = False
                break
        if insert:
            result += elem
    return result


class DelUnicodeSymbols(object):

    def __init__(self, symbols):
        self.symbols = symbols

    def __call__(self, unistr):
        if len(self.symbols) == 0:
            return unistr
        else:
            outstr = u''
            for ch in unistr:
                for delch in self.symbols:
                    if ch == delch:
                        ch = ''
                        break
                outstr += ch
            return outstr


class ReplaceUnicodeSubStrings(object):

    def __init__(self, replaceList):
        self.replaceList = replaceList

    def __call__(self, unistr):
        if len(self.replaceList) == 0:
            return unistr
        else:
            outstr = unistr
            for (old, new) in self.replaceList:
                outstr = outstr.replace(old, new)
            return outstr


class RegexCache(object):

    _cache = {}

    @staticmethod
    def compile(regex):
        pattern = RegexCache._cache.get(regex, None)
        if pattern is None:
            pattern = re.compile(regex, re.U)
            RegexCache._cache[regex] = pattern
        return pattern


class Query:

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        # self.queryName = '_' + str(self.__class__).rsplit('.', 1)[1]
        self.queryName = '_' + className(self, withPath=False)
        self.queryResultProcessingName = self.queryName + '_'

    def getTargetInstance(self, instance):
        targetInstance = instance.parent
        if targetInstance is None:
            raise UndefinedParent
        return targetInstance

    def __call__(self, instance, *args, **kwargs):
        _args = self.args + args
        _kwargs = self.kwargs.copy()
        for key in kwargs:
            _kwargs[key] = kwargs[key]
        targetInstance = self.getTargetInstance(instance)
        query = getattr(targetInstance, self.queryName)
        queryResult = query(*_args, **_kwargs)
        queryResultProcessing = getattr(instance
                                        , self.queryResultProcessingName, None)
        if queryResultProcessing is not None:
            queryResult = queryResultProcessing(queryResult)
        return queryResult


class value(Query):

    def getTargetInstance(self, instance):
        return self

    def _value(self, value):
        return [value]


class valueList(Query):

    def getTargetInstance(self, instance):
        return self

    def _valueList(self, value):
        return value


class xpath(Query):
    pass


class href(Query):
    pass


class title(Query):
    pass


class regex(Query):
    pass


class reValue(Query):
    pass


class tail(Query):
    pass


class text(Query):
    pass


class area(Query):
    pass


class reValueUnit(Query):
    pass


class XpathQueryMixin(object):

    def _xpath(self, xpath):
        return self._elem.xpath(xpath)

    def _attribute(self, attribute, xpath=None):
        if xpath is None or xpath == '':
            xpath = './/'
        if xpath[len(xpath)-1] != '/':
            xpath += '/'
        xpath += 'attribute::'+attribute
        return self._elem.xpath(xpath)

    def _href(self, xpath=None):
        return self._attribute('href', xpath)

    def _title(self, xpath=None):
        return self._attribute('title', xpath)

    def _text(self, text, xpath=None):
        text = normalizeSpace(text)
        if xpath is None or xpath == '':
            xpath = './/'
        if xpath[len(xpath)-1] == '/':
            xpath += '*'
        xpath += "[normalize-space(text())='"+text+"']"
        return self._elem.xpath(xpath)

    def _tail(self, xpath=None):
        result = []
        if xpath is None:
            result.append(self._elem.tail)
        else:
            elems = self._xpath(xpath)
            for elem in elems:
                result.append(elem.tail)
        return result

    def _area(self, xpath, predicate):
        elements = self._xpath(xpath)
        result = []
        predicate = u'self::*' + predicate
        for element in elements:
            if len(element.xpath(predicate)) == 0:
                break
            result.append(element)
        return result


class RegexQueryMixin(object):

    def _regex(self, regex):
        pattern = RegexCache.compile(regex)
        return pattern.findall(self._unicode)

    def _reValue(self, unit, direction=Direction.forward):
        digitRegex = r'([\d.,]+)'
        if direction == Direction.forward:
            regex = digitRegex + r'\s*' + unit
        elif direction == Direction.backward:
            regex = unit + r'\s*' + digitRegex
        else:
            raise DirectionError('Direction should by Direction.forward or' \
                                 + ' Direction.backward')
        return self._regex(regex)

    def _reValueUnit(self, *args):
        # Обработка аргументов
        if len(args) > 2:
            raise TypeError('_reValueUnit takes at most 3 arguments ('\
                            + len(args)+1 + ' given)')
        direction = None
        reUnit = u'[^\W\d]+'
        for arg in args:
            if arg is None:
                pass
            elif type(arg) is int:
                direction = arg
            elif type(arg) is str or type(arg) is unicode:
                reUnit = arg
            else:
                raise TypeError('unexpected argument type')
        reUnit = '(' + reUnit + ')'
        # Формируем regex
        digitRegex = r'([\d.,]+)'
        if direction is None:
            forward = self._reValueUnit(reUnit, Direction.forward)
            backward = self._reValueUnit(reUnit, Direction.backward)
            return forward + backward
        elif direction == Direction.forward:
            regex = digitRegex + r'\s*' + reUnit
        elif direction == Direction.backward:
            regex = reUnit + r'\s*' + digitRegex
        else:
            raise DirectionError('Direction should by Direction.forward or' \
                                 + ' Direction.backward')
        result = self._regex(regex)
        if direction == Direction.backward:
            resultLen = len(result)
            i = 0
            while i < resultLen:
                (unit, value) = result[i]
                result[i] = (value, unit)
                i += 1
        return result


def removeWrapAndClone(elem):
    if type(elem) is list:
        elem = elem[0]
        elem = elem._clone()
        elem._structure = Structure.list
    elif type(elem) is dict:
        key = list(elem.keys())
        key = key[0]
        elem = elem[key]
        elem = elem._clone()
        elem._structure = Structure.dict
        elem._key = key
    else:
        elem = elem._clone()
        elem._structure = Structure.single
    return elem


class ParsBase(object):

    _encoding = 'utf-8'
    _NoneObjectUnicode = u'None'
    _saveInstance = True

    @staticmethod
    def _unicodePostProcessingDefault(unistr):
        return unistr

    _unicodePostProcessing = _unicodePostProcessingDefault

    def __init__(self, query, replaceObj=None, **kwargs):
        self._query = query
        self.parent = None
        self._childNames = set()
        self._structure = Structure.single
        self._name = None
        self._url_local = ''
        self._url_scheme_local = ''
        self._url_netloc_local = ''
        self._url_path_local = ''
        self._url_query_local = ''
        self._url_fragment_local = ''
        self._queryArgs = ()
        self._queryKwargs = {}
        self._key = None
        self._elem = None
        if replaceObj is not None:
            replaceObj = replaceObj._clone()
        self._replaceObj = replaceObj
        self._catcher = kwargs.pop('catcher', None)

    def _replaceAttributes(self, fromInstance):
        self.parent = fromInstance.parent
        self._structure = fromInstance._structure
        self._name = fromInstance._name
        if self._url_scheme_local == '':
            self._url_scheme_local = fromInstance._url_scheme_local
        if self._url_netloc_local == '':
            self._url_netloc_local = fromInstance._url_netloc_local
        if self._url_path_local == '':
            self._url_path_local = fromInstance._url_path_local
        if self._url_query_local == '':
            self._url_query_local = fromInstance._url_query_local
        if self._url_fragment_local == '':
            self._url_fragment_local = fromInstance._url_fragment_local

    def __getattribute__(self, name):
        if name == '_unicode':
            result = object.__getattribute__(self, name)
            return self._unicodePostProcessing(result)
        else:
            return object.__getattribute__(self, name)

    def __getattr__(self, name):
        if name == '_unicodePostProcessing':
            return ParsBase._unicodePostProcessing
        elif name == '_saveInstance':
            return ParsBase._saveInstance
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if name[0] != '_' and name != 'parent': # and isParsStructure(value):
            if not self._childNameExists(name):
                return self._setChild(value, name)
            return object.__setattr__(self, name, value)
        if name in ('_url_local', '_url_scheme_local', '_url_netloc_local'
                    , '_url_path_local', '_url_query_local'
                    , '_url_fragment_local') and value is None:
            value = ''
        if name == '_url_local':
            self._url_scheme, self._url_netloc, self._url_path, self._url_query\
                , self._url_fragment = urlparse.urlsplit(value)
        if name == '_structure':
            if value not in (Structure.single, Structure.list, Structure.dict):
                raise StructureError('Structure should by Structure.single or \
                                    Structure.list')
        object.__setattr__(self, name, value)

    def __unicode__(self):
        if getattr(self, '_elem', None) is None:
            return ParsBase._NoneObjectUnicode
        return self._unicode

    def __str__(self):
        return self.__unicode__().encode(ParsBase._encoding)

    def __repr__(self):
        return self.__str__()

    @property
    def _url(self):
        return urlparse.urlunsplit((self._url_scheme, self._url_netloc,
                                   self._url_path, self._url_query,
                                   self._url_fragment))

    @_url.setter
    def _url(self, value):
        self._url_local = value

    @property
    def _url_scheme(self):
        result = self._url_scheme_local
        if result == '':
            if self.parent is not None:
                result = self.parent._url_scheme
        return result

    @_url_scheme.setter
    def _url_scheme(self, value):
        self._url_scheme_local = value

    @property
    def _url_netloc(self):
        result = self._url_netloc_local
        if result == '':
            if self._url_scheme_local == '':
                if self.parent is not None:
                    result = self.parent._url_netloc
        return result

    @_url_netloc.setter
    def _url_netloc(self, value):
        self._url_netloc_local = value

    @property
    def _url_path(self):
        result = self._url_path_local
        if result == '':
            if self._url_scheme_local == self._url_netloc_local == '':
                if self.parent is not None:
                    result = self.parent._url_path
        return result

    @_url_path.setter
    def _url_path(self, value):
        self._url_path_local = value

    @property
    def _url_query(self):
        result = self._url_query_local
        if result == '':
            if self._url_scheme_local == self._url_netloc_local ==\
                    self._url_path_local == '':
                if self.parent is not None:
                    result = self.parent._url_query
        return result

    @_url_query.setter
    def _url_query(self, value):
        self._url_query_local = value

    @property
    def _url_fragment(self):
        result = self._url_fragment_local
        if result == '':
            if self._url_scheme_local == self._url_netloc_local ==\
                    self._url_path_local == '':
                if self.parent is not None:
                    result = self.parent._url_fragment
        return result

    @_url_fragment.setter
    def _url_fragment(self, value):
        self._url_fragment_local = value

    def _clone(self):
        clone = copy.copy(self)
        clone._childNames = copy.copy(self._childNames)
        if clone._replaceObj is not None:
            clone._replaceObj = clone._replaceObj._clone()
        return clone

    def _childNameExists(self, name):
        if self._replaceObj is not None:
            return self._replaceObj._childNameExists(name)
        return hasattr(self, name)

    def _setChild(self, child, name=None):
        if self._replaceObj is not None:
            return self._replaceObj._setChild(child, name)
        child = removeWrapAndClone(child)
        if name is None:
            name = child._name
        else:
            child._name = name
        if name is None:
            raise UndefinedChildName()
        if name in self._childNames:
            raise DuplicateChildName(name)
        if hasattr(self, name):
            raise SetChildError('Attribute ' + name + ' already exists')
        result = object.__setattr__(self, name, child)
        self._childNames.add(name)
        return result

    def __call__(self, *args, **kwargs):
        clone = self._clone()
        childs = []
        queryArgs = []
        for arg in args:
            if isParsStructure(arg):
                childs.append(arg)
            else:
                queryArgs.append(arg)
        clone._queryArgs = tuple(queryArgs)
        clone._queryKwargs = kwargs
        for child in childs:
            clone._setChild(child)
        return clone

    def _runQuery(self, query=None):
        if query is None:
            query = self._query
        if query is None:
            raise UndefinedQuery
        return query(self, *self._queryArgs, **self._queryKwargs)

    def _processing(self):
        # обработка содержимого _elem в виде:
        # self._elem = some_code(self._elem)
        # по умолчанию обработка не производится
        pass

    def _instanceConstruct(self, queryResult, saveInstance=False):
        saveInstance = saveInstance or self._saveInstance
        catcher = self._catcher
        if catcher is None:
            saveChild = False
        else:
            saveChild = True
        saveChild = saveChild or saveInstance
        instance = self._clone()
        instance._elem = queryResult
        instance._processing()
        if instance._structure == Structure.dict:
            key = instance._key
            if key is not None:
                key.parent = instance
                key = key._construct(saveInstance=True)
                key = key._elem
        if instance._replaceObj is not None:
            instance._replaceObj.parent = instance
            newInstance = instance._replaceObj._construct(saveInstance=True)
            newInstance._replaceAttributes(instance)
            instance = newInstance
        for childName in instance:
            instance[childName].parent = instance
            instance[childName] = instance[childName]._construct(saveChild)
        if catcher is not None:
            catcher(instance)
        if instance._structure == Structure.dict:
            if saveInstance:
                return key, instance
            else:
                return None, None
        else:
            if saveInstance:
                return instance
            else:
                return None

    def _listInstanceConstruct(self, listQueryResult, saveInstance=False):
        saveInstance = saveInstance or self._saveInstance
        instanceList = []
        for queryResult in listQueryResult:
            instance = self._instanceConstruct(queryResult, saveInstance)
            if saveInstance and instance is not None:
                instanceList.append(instance)
        if saveInstance:
            return instanceList
        else:
            return None

    def _dictInstanceConstruct(self, queryResult, saveInstance=False):
        saveInstance = saveInstance or self._saveInstance
        instanceDict = {}
        resultLen = len(queryResult)
        i = 0
        while i < resultLen:
            key, instance = self._instanceConstruct(queryResult[i]
                                                    , saveInstance)
            if key is None:
                key = i
            if saveInstance and instance is not None:
                instanceDict[key] = instance
            i += 1
        if saveInstance:
            return instanceDict
        else:
            return None

    def _prepareQueryResult(self, queryResult):
        # Подготовка результата запроса к обработке. По умолчанию нечего не
        # меняется
        return queryResult

    def _construct(self, saveInstance=False):
        saveInstance = saveInstance or self._saveInstance
        queryResult = self._runQuery()
        queryResult = self._prepareQueryResult(queryResult)
        if type(queryResult) is not list:
            raise BadQueryResult('The query result should be a list')
        if self._structure == Structure.single:
            if len(queryResult) == 0:
                result = NoneObject(self)
            elif len(queryResult) == 1:
                result = self._instanceConstruct(queryResult[0], saveInstance)
            else:
                msg = 'A list of query results must contain one element'
                raise BadQueryResult(msg, queryResult=queryResult
                                     , errorObj=self)
        elif self._structure == Structure.list:
            result = self._listInstanceConstruct(queryResult, saveInstance)
        elif self._structure == Structure.dict:
            result = self._dictInstanceConstruct(queryResult, saveInstance)
        else:
            raise StructureError('Structure should by Structure.single or \
                                 Structure.list or Structure.dict')
        if saveInstance:
            return result
        else:
            return None

    def __iter__(self):
        return iter(self._childNames)

    def __getitem__(self, index):
        return getattr(self, index)

    def __setitem__(self, index, value):
        setattr(self, index, value)

    def __contains__(self, x):
        return x in self._childNames

    def print(self):
        print(self._name, '=', self)

    def printTree(self, level=0, index=None):
        tabSpace = '    '
        indent = tabSpace * level
        indentListSymbol = indent + ' ' * (len(tabSpace) - 2)
        selfStr = self.__str__()
        selfStr = selfStr.replace('\n', '')
        selfName = self._name
        if index is not None:
            if type(index) is unicode:
                index = index.encode(ParsBase._encoding)
            else:
                index = str(index)
            selfName += '[' + index + ']'
        print(indent + selfName + ': ' + selfStr)
        for childName in sorted(self._childNames):
            child = self[childName]
            if type(child) is list:
                print(indentListSymbol + childName + ': [')
                i = 0
                listLen = len(child)
                while i < listLen:
                    child[i].printTree(level+1, i)
                    i += 1
                print(indentListSymbol + ']')
            elif type(child) is dict:
                print(indentListSymbol + childName + ': {')
                for key in sorted(child):
                    child[key].printTree(level+1, key)
                print(indentListSymbol + '}')
            else:
                child.printTree(level+1)

    def printPath(self):
        if self.parent is not None:
            self.parent.printPath()
        string = self._name + ': ' + str(self)
        print(string)


class NoneObject(ParsBase):
    def __init__(self, fromInstance=None):
        query = None
        ParsBase.__init__(self, query)
        if fromInstance is not None:
            self._replaceAttributes(fromInstance)
        self._elem = None

    @property
    def _unicode(self):
        return ParsBase._NoneObjectUnicode


class Container:
    pass


class Processor(object):

    def __init__(self):
        self._result = Container()

    @property
    def result(self):
        return self._result

    def __setattr__(self, name, value):
        if issubclass(value.__class__, ParsBase):
            value._name = name
        object.__setattr__(self, name, value)

    def __call__(self, queryRoot):
        queryRoot = removeWrapAndClone(queryRoot)
        resultName = queryRoot._name
        if resultName is None:
            raise UndefinedQueryRootName()
        result = queryRoot._construct()
        setattr(self._result, resultName, result)


class ElemTypeMixin(object):

    def printElemType(self):
        print(type(self._elem))


class ListElemTypeMixin(object):

    def printElemType(self):
        print('[', end='')
        sep = ''
        for elem in self._elem:
            print(sep, end='')
            print(type(elem), end='')
            sep = ', '
        print(']')


class UnicodeListMixin(object):

    @property
    def _unicode(self):
        unistr = u'[\n'
        for elem in self._elem:
            unistr += etree.tounicode(elem, method='xml', pretty_print=True
                                      , with_tail=False) + u'\n,\n'
        unistr = unistr[0:-2] + u']'
        return unistr


class UnicodeTextMixin(object):

    @property
    def _unicode(self):
        return etree.tounicode(self._elem, method='text', pretty_print=False
                               , with_tail=False)


class UnicodeStrMixin(object):

    @property
    def _unicode(self):
        return self._elem


class UnicodeTreeMixin(object):

    @property
    def _unicode(self):
        return etree.tounicode(self._elem, method='html', pretty_print=True
                               , with_tail=True)


class TreeXpath(UnicodeTreeMixin, XpathQueryMixin, RegexQueryMixin, ParsBase):
    pass


class Str(UnicodeStrMixin, RegexQueryMixin, ParsBase):

    def _processing(self):
        elem = self._elem
        if type(elem) is unicode:
            pass
        if type(elem) is str:
            elem = unicode(elem.decode(ParsBase._encoding))
        else:
            self._elem = unicode(elem)

    @property
    def _unicode(self):
        if self._elem is None:
            return u'None'
        return self._elem


class Value(Str):

    def _processing(self):
        self._elem = self._elem[0]
        Str._processing(self)


class Unit(Str):

    def _processing(self):
        self._elem = self._elem[1]
        Str._processing(self)


class Tail(Str):

    def _processing(self):
        self._elem = unicode(self._elem.tail)


class Text(Str):

    def _processing(self):
        self._elem = unicode(self._elem.text)


class HtmlElements(UnicodeStrMixin, ParsBase):

    def _prepareQueryResult(self, queryResult):
        return [queryResult]

    def _processing(self):
        str = u''
        for elem in self._elem:
            str += etree.tounicode(elem, pretty_print=False,
                                   method='html')
        self._elem = str


class Proxy(object):

    def __init__(self, address, port, type, user=None, password=None):
        self.address = address
        self.port = port
        self.type = type
        self.user = user
        self.password = password
        self.requests = 0
        self.successRequests = 0
        self.failedRequests = 0
        self.failed = False

    def printInfo(self, event):
        def attrStr(self, attr):
            return attr + ': ' + str(getattr(self, attr, None))
        res = 'EVENT: ' + event + '\n'
        res += attrStr(self, 'address') + '\n'
        res += attrStr(self, 'port') + '\n'
        res += attrStr(self, 'type') + '\n'
        res += attrStr(self, 'requests') + '\n'
        res += attrStr(self, 'successRequests') + '\n'
        res += attrStr(self, 'failedRequests') + '\n'
        res += attrStr(self, 'failed') + '\n'
        res += 'proxyCount: ' + str(Web._proxyCount()) + '\n'
        if self.requests == 0:
            res += 'proxyRatio: ' + 'undefined' + '\n'
        else:
            proxyRatio = float(self.successRequests)/float(self.requests)
            res += 'proxyRatio: ' + repr(proxyRatio) + '\n'
        res += '---------------------------------------------\n'
        f = open('var/log/proxyInfo.info', 'a')
        f.write(res)
        f.close()

    def regSuccessRequest(self, url):
        self.printInfo('successStart')
        self.requests += 1
        self.successRequests += 1
        proxy = self.address + ':' + self.port
        event = 'successRequest'
        Web._writeLogProxyEvent(proxy, event, url)
        self.printInfo('successEnd')

    def regFailedRequest(self, url, exception):
        self.printInfo('failedStart')
        self.requests += 1
        self.failedRequests += 1
        if not self.failed and self.failedRequests > Web.maxFailedProxyRequests:
            if self.successRequests == 0:
                self.failed = True
                Web._setProxyFailed(self)
            elif Web._proxyCount() > Web.minimumProxies:
                proxyRatio = float(self.successRequests)/float(self.requests)
                if proxyRatio < Web.proxyRejectRatio:
                    self.failed = True
                    Web._setProxyFailed(self)
        if isinstance(exception, ProxyServerError):
            event = 'proxyServerError'
        elif isinstance(exception, grab.error.GrabTimeoutError):
            event = 'timeoutError'
        elif isinstance(exception, WebClientError) \
                or isinstance(exception, WebServerError):
            httpCode = exception.httpCode
            if httpCode is None:
                httpCode = 'Error'
            else:
                httpCode = str(httpCode)
            event = 'http' + httpCode
        elif isinstance(exception, PageControlFault):
            event = 'pageControlFault'
        elif isinstance(exception, grab.error.GrabConnectionError):
            event = 'connectionError'
        elif isinstance(exception, grab.error.GrabNetworkError):
            event = 'networkError'
        else:
            event = 'failedRequest'
        proxy = self.address + ':' + self.port
        Web._writeLogProxyEvent(proxy, event, url)
        self.printInfo('failedEnd')



class Web(object):

    _proxies = None
    _failedProxies = set()
    proxyFile = None
    hammerTimeouts = None
    randomDelayPeriod = None
    considerTimeoutProxyError = True
    maxFailedProxyRequests = 5
    logDir = None
    proxyStatDir = None
    _proxyStatFileName = None
    _lastReqTime = 0
    attempts = 5
    errorLogDir = None
    _errorLogFilename = None
    serverErrorWait = 120
    minimumProxies = 10
    proxyRejectRatio = 0.5

    @staticmethod
    def _nonePageControlFunc(page, url, proxy):
        pass

    pageControlFunc = _nonePageControlFunc

    @staticmethod
    def getGrabPage(url):
        if Web._proxyMode():
            if Web._proxies is None:
                Web._loadProxies()
            page = Web._getGrabPageProxy(url)
        else:
            page = Web._getGrabPageDirect(url)
        return page

    @staticmethod
    def _getGrabPageProxy(url, exceptProxy=None, waitCnt=0):
        if exceptProxy is None:
            exceptProxy = set()
        # ---------------------------------------
        string = '{'
        for pr in exceptProxy:
            string += pr.address + ':' + pr.port + ', '
        string += '}\n'
        f = open('var/log/exceptProxy.log', 'a')
        f.write(string)
        f.close()
        # ---------------------------------------
        proxy = Web._nextProxy(exceptProxy)
        kwargs = {}
        kwargs['proxy'] = proxy.address + ':' + proxy.port
        kwargs['proxy_type'] = proxy.type
        if proxy.user is not None:
            password = proxy.password
            if password is None:
                password = ''
            kwargs['proxy_userpwd'] = proxy.user + ':' + password
        try:
            page = Web._loadGrabPage(url, proxy, **kwargs)
            proxy.regSuccessRequest(url)
        except (WebError, grab.error.GrabNetworkError) as e:
            exceptProxy.add(proxy)
            if isinstance(e, WebError):
                if e.httpCode in (503, 507, 509):
                    if waitCnt > 0:
                        time.sleep(Web.serverErrorWait)
                    waitCnt += 1
            try:
                page = Web._getGrabPageProxy(url, exceptProxy, waitCnt)
                proxy.regFailedRequest(url, e)
            except AllProxyAlreadyUsed as e1:
                exceptionList = getattr(e1, 'exceptionList', None)
                if exceptionList is None:
                    e1.exceptionList = []
                e1.exceptionList.append(e)
                raise e1
        return page

    @staticmethod
    def _getGrabPageDirect(url):
        attempts = Web.attempts
        if attempts is None:
            attempts = 1
        i = 0
        while i < attempts:
            try:
                return Web._loadGrabPage(url)
            except grab.error.GrabNetworkError as e:
                i += 1
                if i < attempts:
                    time.sleep(Web.serverErrorWait)
                    continue
                raise
            except WebServerError as e:
                if e.httpCode in (503, 507, 509):
                    i += 1
                    if i < attempts:
                        time.sleep(Web.serverErrorWait)
                        continue
                raise

    @staticmethod
    def _nextProxy(exceptProxy=None):
        if exceptProxy is None:
            exceptProxy = set()
        if Web._proxies is None:
            raise ProxyError('set of proxy servers is not initialized')
        if len(Web._proxies) == 0:
            if len(Web._failedProxies) == 0:
                raise ProxyError('set of proxy servers is empty')
            else:
                raise ProxyError('All proxy servers is failed')
        proxies = Web._proxies - exceptProxy
        if len(proxies) == 0:
            raise AllProxyAlreadyUsed()
        return reduce((lambda x, y: x if x.requests <= y.requests else y)
                      , proxies)

    @staticmethod
    def _setProxyFailed(proxy):
        Web._failedProxies.add(proxy)
        Web._proxies.remove(proxy)

    @staticmethod
    def _writeLogError(url, proxy, exception):
        if Web.errorLogDir is None:
            return
        fileName = Web._errorLogFilename
        if fileName is None:
            dateName = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            fileName = dateName + '.' + str(os.getpid()) + '.neterror.log'
            fileName = Web.errorLogDir + fileName
            Web._errorLogFilename = fileName
        logFile = open(fileName, 'a')
        if proxy is not None:
            proxy = proxy.address + ':' + proxy.port
        else:
            proxy = 'None'
        proxy = '<<PROXY>> ' + proxy
        error = '<<ERROR>> '
        error += className(exception) + ': '
        error += str(exception)
        url = '<<URL>> ' + '"' + url + '"'
        string = proxy + '\t' + error + '\t' + url
        string = delElements(string, '\r\n')
        string += '\n'
        logFile.write(string)
        logFile.close()

    @staticmethod
    def _writeLogProxyEvent(proxy, event, url):
        if Web.proxyStatDir is None:
            return
        fileName = Web._proxyStatFileName
        if fileName is None:
            dateName = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            fileName = dateName + '.' + str(os.getpid()) + '.proxystat'
            fileName = Web.proxyStatDir + fileName
            Web._proxyStatFileName = fileName
        statFile = open(fileName, 'a')
        string = '<' + event + '>\t' + proxy + '\t' + '"' + url + '"' + '\n'
        statFile.write(string)
        statFile.close()

    @staticmethod
    def _proxyMode():
        return (Web._proxies is not None) or (Web.proxyFile is not None)

    @staticmethod
    def _loadProxies():
        proxyFile = open(Web.proxyFile, 'r')
        if Web.logDir is not None:
            wrongTypeFile = open(Web.logDir+'wrongProxyType.log', 'a')
        proxies = set()
        typePattern = RegexCache.compile(r'^(\w+)\t[\w:@.]+\s*$')
        addrPortPattern = RegexCache.compile(r'[\t@]([\w.]+):(\d+)\s*$')
        usrPwdRegex = r'^\w+\t(\w+):([^\s:]+)*@[\w.]+:\d+\s*$'
        usrPwdPattern = RegexCache.compile(usrPwdRegex)
        for line in proxyFile:
            proxyType = typePattern.findall(line)[0]
            proxyType = proxyType.lower()
            if proxyType not in ('http', 'socks4', 'socks5'):
                if Web.logDir is not None:
                    wrongTypeFile.write(Web.proxyFile + ' : ' + line)
                continue
            proxyAddress, proxyPort = addrPortPattern.findall(line)[0]
            usrPwd = usrPwdPattern.findall(line)
            if len(usrPwd) == 1:
                proxyUser, proxyPassword = usrPwd[0]
            else:
                proxyUser = proxyPassword = None
            proxy = Proxy(proxyAddress, proxyPort, proxyType
                          , proxyUser, proxyPassword)
            proxies.add(proxy)
        Web._proxies = proxies
        if Web.logDir is not None:
            wrongTypeFile.close()
        proxyFile.close()

    @staticmethod
    def _lastRequestTime(setTime=None):
        if setTime is None:
            return Web._lastReqTime
        else:
            Web._lastReqTime = setTime
            return setTime

    @staticmethod
    def _proxyCount():
        return len(Web._proxies)

    @staticmethod
    def _grabPageHttpCodeCheck(page, proxy):
        response = getattr(page, 'response', None)
        code = None
        body = None
        if response is not None:
            code = getattr(response, 'code', None)
            body = getattr(response, 'body', None)
        if 100 <= code < 200:
            raise WebClientError(httpCode=code, httpBody=body)
        elif 200 <= code < 300:
            if code == 206:
                raise WebClientError(httpCode=code, httpBody=body)
            return
        elif 300 <= code < 400:
            raise WebClientError(httpCode=code, httpBody=body)
        elif 400 <= code < 500:
            raise WebClientError(httpCode=code, httpBody=body)
        elif 500 <= code < 600:
            if code == 504 and proxy is not None:
                raise ProxyServerError(httpCode=code, httpBody=body)
            raise WebServerError(httpCode=code, httpBody=body)
        else:
            raise WebClientError(httpCode=code, httpBody=body)

    @staticmethod
    def _loadGrabPage(url, currentProxy=None, **kwargs):
        # _breaker(5)
        kwargs = kwargs.copy()
        page = Grab()
        if Web.hammerTimeouts is not None:
            kwargs['hammer_mode'] = True
            kwargs['hammer_timeouts'] = Web.hammerTimeouts
        if len(kwargs) > 0:
            page.setup(**kwargs)
        if Web.randomDelayPeriod is not None:
            delaySecFrom, delaySecTo = Web.randomDelayPeriod
            sleepSeconds = random.uniform(delaySecFrom, delaySecTo)
            timeFromLastRequest = time.time() - Web._lastRequestTime()
            sleepSeconds -= timeFromLastRequest
            if sleepSeconds > 0:
                time.sleep(sleepSeconds)
        try:
            try:
                requestTime = time.time()
                page.go(url)
                Web._grabPageHttpCodeCheck(page, currentProxy)
                Web.pageControlFunc(page, url, currentProxy)
                Web._lastRequestTime(requestTime)
            except grab.error.GrabConnectionError as e:
                if currentProxy is None:
                    raise
                strerror = getattr(e, 'strerror', None)
                if strerror is not None:
                    if strerror.find(currentProxy.address) != -1:
                        raise ProxyServerError(originalException=e)
                    if strerror.startswith(r"Can't complete SOCKS4 connection to"):
                        raise ProxyServerError(originalException=e)
                    if strerror.startswith(r"Failed to receive SOCKS4"):
                        raise ProxyServerError(originalException=e)
                raise
        except (grab.error.GrabNetworkError, WebError) as e:
            Web._writeLogError(url, currentProxy, e)
            raise
        return page


class PageCache(object):

    _cache = {}
    cacheDir = None
    _fileMapLoaded = False
    _fileMapName = 'url_file.map'
    _maxFileNumber = None
    memoryCache = True

    @staticmethod
    def getPage(url, withoutCache=False):
        url = normalizeUrl(url)
        container = PageCache._cache.get(url, None)
        if container is None:
            container = Container()
            page, fileName = PageCache._loadPage(url, withoutCache)
            if PageCache.memoryCache:
                container.page = page
            else:
                container.page = None
            container.fileName = fileName
            PageCache._cache[url] = container
        else:
            if container.page is None or withoutCache:
                page, fileName = PageCache._loadPage(url, withoutCache)
                if PageCache.memoryCache:
                    container.page = page
                else:
                    container.page = None
                container.fileName = fileName
            else:
                page = container.page
        return page

    @staticmethod
    def _webLoad(url):
        page = Web.getGrabPage(url)
        return page

    @staticmethod
    def _webLoadSaveFile(url):
        page = PageCache._webLoad(url)
        fileName = PageCache._writePage(page)
        PageCache._saveFileMap(url, fileName)
        return page, fileName

    @staticmethod
    def _path(fileName):
        path = PageCache.cacheDir
        if path[len(path)-1] != '/':
            path += '/'
        path += fileName
        return path

    @staticmethod
    def _findMaxFileNumber():
        if not PageCache._fileMapLoaded:
            PageCache._loadFileMap()
        cache = PageCache._cache
        maxNumber = 0
        for url in cache:
            container = cache[url]
            if container is not None:
                fileName = container.fileName
                if fileName is not None:
                    fileNumber = int(fileName[:-7])  # обрезаем .pickle
                    if maxNumber < fileNumber:
                        maxNumber = fileNumber
        return maxNumber

    @staticmethod
    def _createFileName():
        if not PageCache._fileMapLoaded:
            PageCache._loadFileMap()
        maxFileNumber = PageCache._maxFileNumber
        if maxFileNumber is None:
            maxFileNumber = PageCache._findMaxFileNumber()
        maxFileNumber += 1
        fileName = str(maxFileNumber) + '.pickle'
        PageCache._maxFileNumber = maxFileNumber
        return fileName

    @staticmethod
    def _writePage(page, fileName=None):
        if fileName is None:
            fileName = PageCache._createFileName()
        path = PageCache._path(fileName)
        pageFile = open(path, 'w')
        pagePickler = pickle.Pickler(pageFile, pickle.HIGHEST_PROTOCOL)
        pagePickler.dump(page)
        pageFile.close()
        return fileName

    @staticmethod
    def _readPage(fileName):
        path = PageCache._path(fileName)
        try:
            pageFile = open(path, 'r')
        except IOError as e:
            if e.errno == 2:
                return None
            else:
                raise
        try:
            pageUnpickler = pickle.Unpickler(pageFile)
            page = pageUnpickler.load()
        except Exception:
            return None
        pageFile.close()
        return page

    @staticmethod
    def _loadFileMap():
        path = PageCache._path(PageCache._fileMapName)
        try:
            mapFile = open(path, 'r')
        except IOError as e:
            if e.errno == 2:
                return
            else:
                raise
        for mapString in mapFile:
            split = mapString.split('\t')
            url = split[0]
            url = normalizeUrl(url)
            fileName = split[1][:-1]  # обрезаем \n
            container = Container()
            container.page = None
            container.fileName = fileName
            PageCache._cache[url] = container

    @staticmethod
    def _saveFileMap(url, fileName):
        path = PageCache._path(PageCache._fileMapName)
        mapFile = open(path, 'a')
        mapString = '{0}\t{1}\n'.format(url, fileName)
        mapFile.write(mapString)
        mapFile.close()

    @staticmethod
    def _fileLoad(url, withoutCache=False):
        if not PageCache._fileMapLoaded:
            PageCache._loadFileMap()
            PageCache._fileMapLoaded = True
        container = PageCache._cache.get(url, None)
        if withoutCache:
            if container is None:
                page, fileName = PageCache._webLoadSaveFile(url)
            elif container.fileName is None:
                page, fileName = PageCache._webLoadSaveFile(url)
            else:
                fileName = container.fileName
                page = PageCache._webLoad(url)
                fileName = PageCache._writePage(page, fileName)
        elif container is None:
            page, fileName = PageCache._webLoadSaveFile(url)
        elif container.page is None and container.fileName is None:
            page, fileName = PageCache._webLoadSaveFile(url)
        elif container.page is None:
            fileName = container.fileName
            page = PageCache._readPage(fileName)
            if page is None:
                page = PageCache._webLoad(url)
                fileName = PageCache._writePage(page, fileName)
        elif container.fileName is None:
            page = container.page
            fileName = PageCache._writePage(page)
            PageCache._saveFileMap(url, fileName)
        else:
            page = container.page
            fileName = container.fileName
        return page, fileName

    @staticmethod
    def _mkdir(dirPath):
        path = PageCache.cacheDir
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise PageCacheError('Cannot create dir: '+path)
        else:
            os.makedirs(path)

    @staticmethod
    def _loadPage(url, withoutCache=False):
        if PageCache.cacheDir is None:
            fileName = None
            page = PageCache._webLoad(url)
        else:
            PageCache._mkdir(PageCache.cacheDir)
            page, fileName = PageCache._fileLoad(url, withoutCache)
        return page, fileName

    @staticmethod
    def _proxyType():
        regex = u'/proxy-([^.]+)\.list$'
        pattern = RegexCache.compile(regex)
        proxyType = pattern.findall(PageCache.proxyFile.strip())[0]
        if proxyType in ('http', 'socks4', 'socks5'):
            return proxyType
        else:
            raise PageCacheError('proxy server type: ' + proxyType\
                                 + ' is invalid')


class Url(Str):

    def _processing(self):
        Str._processing(self)
        self._url = self._elem
        self._elem = self._url


class Page(XpathQueryMixin, UnicodeTreeMixin, ParsBase):

    attempts = 5
    errorWait = 60
    urlControl = False

    def __init__(self, *args, **kwargs):
        self._urlControl = kwargs.pop('urlControl', None)
        ParsBase.__init__(self, *args, **kwargs)

    def _processing(self):
        self._url = normalizeUrl(self._elem)
        i = 0
        withoutCache = False
        urlControl = self._urlControl
        if urlControl is None:
            urlControl = Page.urlControl
        selfUrl = normalizeUrl(self._url)
        while i < Page.attempts:
            i += 1
            page = PageCache.getPage(selfUrl, withoutCache)
            pageUrl = getattr(page.response, 'url', selfUrl)
            pageUrl = normalizeUrl(pageUrl)
            if urlControl and selfUrl.lower() != pageUrl.lower():
                if i >= Page.attempts:
                    msg = 'Fails urlControl'
                    raise PageError(msg, grabPage=page)
                else:
                    withoutCache = True
                    time.sleep(Page.errorWait)
            else:
                self._elem = page.xpath('/*')
                return
        msg = 'Fails urlControl'
        raise PageError(msg, grabPage=page)


class PagerMixin(object):

    def _listInstanceConstruct(self, queryResult, saveInstance=False):
        saveInstance = saveInstance or self._saveInstance
        queryResult = queryResult[0]
        url = queryResult['startUrl']
        href = queryResult['href']
        resultList = []
        while True:
            page = self._instanceConstruct(url, saveInstance=True)
            if saveInstance:
                resultList.append(page)
            if self._breakPage(page):
                break
            url = page._href(href)
            if len(url) == 0:
                break
            if len(url) > 1:
                raise BadQueryResult('Query "href" returned more than one value')
            url = url[0]
        if saveInstance:
            return resultList
        else:
            return None

    def _breakPage(self, page):
        return False


class Pager(PagerMixin, Page):
    pass


class KeyStr(Str):

    def _processing(self):
        Str._processing(self)
        self._elem = normalizeSpace(self._elem)


class KeyText(Text):

    def _processing(self):
        Text._processing(self)
        self._elem = normalizeSpace(self._elem)


# -------------------------------------------------------------------------

@staticmethod
def urlControl(page, url, proxy):
    response = getattr(page, 'response', None)
    if response is None:
        return
    requestUrl = normalizeUrl(url)
    responseUrl = normalizeUrl(getattr(response, 'url', requestUrl))
    if requestUrl.lower() != responseUrl.lower():
        httpCode = getattr(response, 'code', None)
        httpBody = getattr(response, 'body', None)
        raise PageControlFault(httpCode=httpCode, httpBody=httpBody
                               , requestUrl=requestUrl, responseUrl=responseUrl)

def printTreeCatcher(instance):
    instance.printTree()


if __name__ == '__main__':
    pass
