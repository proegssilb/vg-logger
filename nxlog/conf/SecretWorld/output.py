#!/usr/bin/env python3

import json as j
import datetime as dt
import random as r
import os.path as pth
from multiprocessing import Process, Manager
import concurrent.futures as ft
from functools import partial
import queue as q
import sys, logging, logging.config, logging.handlers

import argh
import fileinput as fi
import gmpy2 as g
from requests_futures.sessions import FuturesSession

def jsonDecoderHook(objParsed):
    if 'EventReceivedTime' in objParsed:
        evReceived = dt.datetime.strptime(objParsed['EventReceivedTime'], '%Y-%m-%d %H:%M:%S')
        objParsed['EventReceivedTime'] = evReceived

    if 'etime' in objParsed:
        evReceived = dt.datetime.strptime(objParsed['etime'], '%Y-%m-%d %H:%M:%S')
        objParsed['etime'] = evReceived

    if 'ParsedTime' in objParsed:
        # Convert the ParsedTime into a datetime, and deal with midnight case.
        logname = objParsed['filename']
        first, time = logname.split('_')
        hour, minute = int(time[:2]), int(time[2:4])
        _, year, month, day = first.split('-')
        year, month, day = int(year), int(month), int(day)
        mindate = dt.datetime(year, month, day, hour, minute)
        hh, mm, ss = objParsed['ParsedTime'].split(':')
        hh, mm, ss = int(hh), int(mm), int(ss)
        guesstime = dt.datetime(year, month, day, hh, mm, ss)
        while guesstime < mindate:
            guesstime += dt.timedelta(1)    # Add a day.
        objParsed['etime'] = guesstime
        del objParsed['ParsedTime']

    # Cleanup
    if 'PatternName' in objParsed: del objParsed['PatternName']
    if 'SourceModuleName' in objParsed: del objParsed['SourceModuleName']
    if 'SourceModuleType' in objParsed: del objParsed['SourceModuleType']
    return objParsed


class JsonDateEncoder(j.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()
        return JSONEncoder.default()


def asciiDateStamp(time=dt.datetime.now()):
    """Generate a Base 62 encoding of current time in milliseconds."""
    return g.digits(int(time.timestamp()*1000), 62)


def generateUniqifier(length=3, base=62):
    maxval = base**length-1
    return g.digits(r.randint(1,maxval), base)

class SystemState:
    """Represents current state, handles state persistence."""
    PlayerName = ''
    zoneName = ''
    encounterId = ''

    couchDbUrl = 'localhost:5984'
    autosave = True
    dbName = ''
    docName = ''

    def __init__(self, couchDbUrl='localhost:5984', dbName='tsw-parse-state',
                 docname='_currentState', prevstate=None, autoload=True,
                 autosave=True):
        """Initialize the SystemState. prevstate overrides loading process,
        autoload/autosave switch on/off interactions with CouchDB."""
        self.autosave = autosave
        self.couchDbUrl = couchDbUrl
        self.dbName = dbName
        self.docName = docname
        if autoload:
            self.load()
        if prevstate:
            self.PlayerName = prevstate['PlayerName']
            self.zoneName = prevstate['zoneName']
            self.encounterId = prevstate['encounterId']

    def save(self):
        pass

    def load(self):
        pass

    def processMessage(self, message):
        # This might be the easiest place to spot encounterId changes,
        # but it's not the right place to do something about them.
        patternId = message["PatternID"]
        rv = SystemState(self.couchDbUrl, self.dbName, self.docName, {
            'PlayerName': self.PlayerName,
            'zoneName': self.zoneName,
            'encounterId': self.encounterId
        }, False, self.autosave)
        if patternId == 2003:
            rv.encounterId = asciiDateStamp(message['etime'])
        elif patternId == 2002:
            rv.encounterId = asciiDateStamp(message['etime'])
            rv.PlayerName = message["PlayerName"] or self.PlayerName
        elif patternId == 2001:
            rv.zoneName = message['ZoneName']
            rv.encounterId = asciiDateStamp(message['etime'])
        elif patternId == 2000:
            rv.PlayerName = message["PlayerName"] or self.PlayerName
        return rv


decoder = j.JSONDecoder(object_hook=jsonDecoderHook)
encoder = JsonDateEncoder()


def parse(message, systemState=None):
    """Parse and enrich the given message, as would be parsed by nxlog."""
    logger = logging.getLogger("nxlog.vg-tsw.parse")
    message = decoder.decode(message)
    if not 'PatternID' in message:
        logmsg = 'Message was not parsed by nxlog: {}'.format(message)
        logger.error(logmsg)
        return (None, systemState)

    if isinstance(systemState, str):
        prevstate = j.loads(systemState)
        systemState = SystemState(autoload=False, prevstate=prevstate)
    newState = systemState.processMessage(message)
    message['PlayerName'] = newState.PlayerName
    message['ZoneName'] = newState.zoneName

    # Get encid set correctly
    if message['PatternID'] == 2003:
        message['encid'] = systemState.encounterId
    elif newState.encounterId is not None and message['PatternID'] not in (2000, 2001):
        message['encid'] = newState.encounterId

    # Set _id
    uniqifier = generateUniqifier()
    etime = message.get('etime', dt.datetime.now())
    if message['PatternID'] < 2000:
        encid = message.get('encid', 'No.Encounter')
        attacker = message.get('attackername', 'no.attacker')
        idParts = [encid, attacker, asciiDateStamp(etime), message['type'], uniqifier]
        if None in idParts: print("Null in id:", idParts, message)
        message['_id'] = '-'.join(idParts)
    else:
        message['_id'] = '-'.join([message['type'], asciiDateStamp(etime),
                                   uniqifier])

    if 'filename' in message:
        del message['filename']

    if 'PatternID' in message:
        del message['PatternID']

    return message, newState


def savestream(*fileNames, couchDbUrl='http://localhost:5984',
               elasticUrl='http://localhost:9200', dbName='test'):
    """Take a stream of messages, parse them, and save them."""
    logger = logging.getLogger("nxlog.vg-tsw")
    state = SystemState(autoload=False, autosave=False)
    queueMan = Manager()
    esQueue = queueMan.Queue(2000)
    cdbQueue = queueMan.Queue(2000)
    exitEvent = queueMan.Event()
    esProc = esWorker(elasticUrl, dbName, esQueue, exitEvent)
    cdbProc = cdbWorker(couchDbUrl, dbName, cdbQueue, exitEvent)
    esProc.start(), cdbProc.start()
    with fi.input(fileNames) as inputs:
        for line in inputs:
            try:
                outLine, state = parse(line, state)
                if outLine is None: continue
                esQueue.put(outLine)
                cdbQueue.put(outLine)
            except Exception as exc:
                exc_info = (type(exc), exc, exc.__traceback__)
                logger.error('Exception while parsing line:', exc_info=exc_info)
                continue
    exitEvent.set()
    esProc.join()
    cdbProc.join()


def esTranformer(message):
    return message

def esWorker(url, dbName, queue, exitEvent):
    writeFunc = partial(storeToElastic, dbUrl=url, dbname=dbName)
    return Process(target=writerThread, args=(queue, exitEvent, esTranformer,
                                              writeFunc))


def cdbTranformer(message):
    return message

def cdbWorker(url, dbName, queue, exitEvent):
    writeFunc = partial(storeToCouchDb, dbUrl=url, dbname=dbName)
    return Process(target=writerThread, args=(queue, exitEvent, cdbTranformer,
                                              writeFunc))

def store(message, couchDbUrl, elasticUrl, dbName='test'):
    """Store a message, as enriched by parse()."""
    cFut = storeToCouchDb([message], dbUrl=couchDbUrl, dbName=dbName)
    eFut = storeToElastic([message], dbUrl=elasticUrl, dbName=dbName)
    ft.wait([cFut, eFut])

executor = ft.ThreadPoolExecutor(max_workers=8)
httpSession = FuturesSession(executor=executor)

def storeToCouchDb(messages, dbUrl="http://localhost:5984", dbname='test', tries=3):
    logger = logging.getLogger("nxlog.vg-tsw.couchdb")
    jsonBody = {"docs": messages}
    url = "{}/{}/_bulk_docs"
    url = url.format(dbUrl, dbname)
    rv = httpSession.post(url, data=encoder.encode(jsonBody), headers={'Content-Type': 'application/json'})
    def callback(fut):
        if (fut.cancelled()):
            return
        response = fut.result()
        if response.status_code in (400, 500):
            # Best we can do is log and continue
            message = 'Invalid request ({}) : (Response {}) (Request {})'
            message = message.format(response.status_code, response.content, jsonBody)
            logger.error(message)
            return
        respJson = {}
        try:
            respJson = response.json()
        except Exception:
            logger.error("Failed to parse JSON response: {}".format(response.content()))
        if not isinstance(respJson, list):
            logger.error("Response did not parse to list: {}".format(respJson))
        for obj, resp in zip(messages, respJson):
            if 'ok' in resp and resp['ok']:
                continue
            if 'error' in resp and resp['error'] == 'conflict':
                if tries > 0:
                    idparts = obj['_id'].split('-')
                    idparts[-1] = generateUniqifier()
                    newId = '-'.join(idparts)
                    logger.warning('Re-submitting message under new ID: {} | {}'.format(obj, newId))
                    obj['_id'] = newId
                    storeToCouchDb([obj], dbUrl, dbname, tries-1)
                else:
                    logger.error('Hit max number of conflict retries: {}'.format(obj))
            logger.error('Error in submitting message: {} | {}'.format(obj, resp))
    rv.add_done_callback(callback)
    return rv

def storeToElastic(messages, dbUrl="http://localhost:9200", dbname='test'):
    logger = logging.getLogger("nxlog.vg-tsw.elasticsearch")
    content = ""
    for msg in messages:
        meta = {'index': {'_index': dbname,
                          '_type': msg['type'],
                          '_id': msg['_id']}
                }
        del msg['_id']
        content = '\n'.join([content, encoder.encode(meta), encoder.encode(msg)])
    content = content + '\n'
    url = '/'.join([dbUrl, dbname, '_bulk'])
    rv = httpSession.post(url, data=content)
    def callback(fut):
        if (fut.cancelled()):
            return
        response = fut.result()
        if response.status_code not in (200, 201):
            logger.error("Did not receive successful response: {} ({})"
                         .format(response.status_code, response.content()))
            return
        respJson = {}
        try:
            respJson = response.json()
        except Exception:
            logger.error("Failed to parse JSON response: {}".format(response.content()))
        if not respJson['errors']:
            return
        for msg, res in zip(messages, respJson['items']):
            if res['index']['status'] in (200, 201):
                continue
            logger.error("Failed to index doc: ({}) {}".format(
                res['index']['result'], doc))
    rv.add_done_callback(callback)
    return rv


def writerThread(queue, exitEvent, transformer, writeFunc, batchSize=500,
                 timeout=3):
    buff = []
    futures = []
    lastEvent = dt.datetime.now()
    while not exitEvent.is_set():
        try:
            msg = queue.get(timeout=timeout)
        except q.Empty:
            if (dt.datetime.now() - lastEvent).total_seconds() > timeout and len(buff) > 0:
                futures.append(writeFunc(buff))
                buff = []
        else:
            buff.append(transformer(msg))
            lastEvent = dt.datetime.now()
            if len(buff) > batchSize:
                futures.append(writeFunc(buff))
                buff = []

        if len(futures) > 0:
            done = [f for f in futures if f.done()]
            futures = [f for f in futures if not f.done()]

    # If we're exiting, make sure that we don't lose anything.
    if len(buff) > 0:
        futures.append(writeFunc(buff))
    ft.wait(futures)


if __name__ == '__main__':
    config = None
    with open('C:\\ProgramData\\nxlog\\conf\\SecretWorld\\pylogging.json', 'r') as confFile:
        config = j.load(confFile)
    logging.config.dictConfig(config)
    argh.dispatch_commands([store, savestream, parse])
