__author__ = 'github.com/phowat'

import tornado.web
from tornado.websocket import WebSocketHandler
from tornado.ioloop import IOLoop
import Queue
import json
from itertools import cycle
import base64
from passlib.apps import custom_app_context as sins_context

sessions = {}
queues = {}
pairs = {}

class SSession(object):
    def __init__(self, write_message):
        self.topics = []
        self.__write_message = write_message
        print "Created Session."
    def write_message(self, destination, content):
        message = {
            'destination': destination,
            'content': content,
        }
        self.__write_message(json.dumps(message))

class SQueue(object):
    def __init__(self, name):
        self.listeners = []
        self.messages = Queue.Queue()
        self.current = None
        self.cycle = None
        self.name = name

class SPairSide(object):
    def __init__(self):
        self.key = None
        self.messages = Queue.Queue()

class SPair(object):
    def __init__(self, name):
        self.a_side = SPairSide()
        self.b_side = SPairSide()
        self.name = name
        
class SinsHandler(WebSocketHandler):

    def __get_request_key(self):
        return self.request.headers['Sec-Websocket-Key'].strip('=')

    def __prepare_queue(self, name):
        try:
            queue = queues[name]
        except:
            queues[name] = SQueue(name)
            queue = queues[name]
        return queue

    def open(self):
        self.key = self.__get_request_key()
        print "Opening connection %s" % (self.key)
        sessions[self.key] = SSession(self.write_message)

    def on_close(self):
        #TODO: Maybe we need to clean up queues here also, test this.
        #TODO: Refactor

        #TODO: This could be optimized. Maybe I could track in the session if
        # it is part of a pair.
        p_key = None
        for key in pairs:
            if pairs[key].a_side.key == self.key:
                p_key = key
                side = pairs[key].b_side
                name = pairs[key].name
                break
            elif pairs[key].b_side.key == self.key:
                p_key = key
                side = pairs[key].a_side
                name = pairs[key].name
                break
                
        if p_key is not None:
            sessions[side.key].write_message(
                    "/pair/"+name, json.dumps({"command": "DISCONNECT"}))
            del(pairs[p_key])

    def __on_subscribe(self, dest_type, dest_name):
        if dest_type == 'topic':
            session = sessions[self.key]
            if dest_name not in session.topics:
                session.topics.append(dest_name)

        elif dest_type == 'queue':

            queue = self.__prepare_queue(dest_name)
            if self.key not in queue.listeners:
                queue.listeners.append(self.key)

            queue.cycle = cycle(queue.listeners)
            if queue.current is not None:
                while queue.current != queue.cycle.next():
                    continue

            if not queue.messages.empty():
               while True:
                    try:
                        msg = queue.messages.get_nowait()
                    except Queue.Empty:
                        break
                    session.write_message("/queue/"+dest_name, msg)
        elif dest_type == 'pair':
            try:
                pair = pairs[dest_name]
                side = "a"
            except:
                pairs[dest_name] = SPair(dest_name)
                pair = pairs[dest_name]
                side = "b"

            if side == "a":
                side = pair.a_side
            elif pair.b_side.key is None:
                side = pair.b_side
            else:
                print "Connection already paired."
                # TODO: Inform failure to client.
            side.key = self.key
            if not side.messages.empty():
               while True:
                    try:
                        msg = side.messages.get_nowait()
                    except Queue.Empty:
                        break
                    session.write_message("/pair/"+dest_name, msg)
        else:
            print "Unknown destination type "+dest_type +"."

    def __on_unsubscribe(self, dest_type, dest_name):
        if dest_type == 'topic':
            session = sessions[self.key]
            if dest_name not in session.topics:
                session.topics.remove(dest_name)

        elif dest_type == 'queue':
            try:
                queue = queues[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            queue.listeners.remove(self.key)
            cur_listener = queue.current
            if cur_listener == self.key:
                cur_listener = queue.cycle.next()
            queue.cycle = cycle(queue.listeners)

            if len(queue.listeners) < 1 and queue.messages.empty():
                del(queues[dest_name])
            elif cur_listener is not None:
                while cur_listener != queue.cycle.next():
                    continue

        elif dest_type == 'pair':
            #TODO: Refactor
            try:
                pair = pairs[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            if pair.a_side.key == self.key:
                sessions[pair.b_side.key].write_message(
                    "/pair/"+pair.name, json.dumps({"command": "DISCONNECT"}))
            elif pair.a_side.key == self.key:
                sessions[pair.a_side.key].write_message(
                    "/pair/"+pair.name, json.dumps({"command": "DISCONNECT"}))
            else:
                print "Unsubscribe from party not subscribed."
                return

            del(pairs[dest_name])

        else:
            print "Unknown destination type "+dest_type +"."

    def __on_send(self, dest_type, dest_name, content):
        print "Send message [%s] to destination /%s/%s" % \
              (content, dest_type, dest_name)
        if dest_type == 'topic':
            for sname in sessions:
                if dest_name in sessions[sname].topics:
                    sessions[sname].write_message("/topic/"+dest_name, content)

        elif dest_type == 'queue':
            queue = self.__prepare_queue(dest_name)
            if len(queues[dest_name].listeners) > 0:
                current = queue.cycle.next()
                sessions[current].write_message("/queue/"+dest_name, content)
                queue.current = current
            else:
                queue.messages.put_nowait(content)

        elif dest_type == 'pair':
            #TODO: Refactor
            #TODO: Pairs should support receiveing messages while the other
            # side hasn't connected

            try:
                pair = pairs[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            if pair.a_side.key == self.key:
                side = pair.b_side
            elif pair.b_side.key == self.key:
                side = pair.a_side
            else:
                print "Message from endpoint not in this pair."
                return

            if side.key is not None:
                sessions[side.key].write_message("/pair/"+dest_name,content)
            else:
                side.messages.put_nowait(content)
                

    def on_message(self, message):
        info = None
        try:
            info = json.loads(message)
            command = info['command']
        except:
            print "Malformed JSON object received."
            return

        print "Got Message: [%s]." % (message)

        (__slash, dest_type, dest_name) = info['destination'].split('/')
        if command == 'SEND':
            self.__on_send(dest_type, dest_name, info['content'])

        elif command == 'SUBSCRIBE':
            self.__on_subscribe(dest_type, dest_name)

        elif command == 'UNSUBSCRIBE':
            self.__on_unsubscribe(dest_type, dest_name)

        elif command == 'DISCONNECT':
            print 'DISCONNECT'
        else:
            print "Unknown command "+str(command)

# Auth code "stolen" from: http://kelleyk.com/post/7362319243/easy-basic-http-authentication-with-tornado
def require_basic_auth(handler_class):
    def wrap_execute(handler_execute):
        def require_basic_auth(handler, kwargs):
            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                handler.set_status(401)
                handler.set_header('WWW-Authenticate', 'Basic realm=Restricted')
                handler._transforms = []
                handler.finish()
                return False
            auth_decoded = base64.decodestring(auth_header[6:])
            kwargs['basicauth_user'], kwargs['basicauth_pass'] = auth_decoded.split(':', 2)
            return True
        def _execute(self, transforms, *args, **kwargs):
            if not require_basic_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)
        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class

@require_basic_auth
class StatsHandler(tornado.web.RequestHandler):
    def get(self, basicauth_user, basicauth_pass):
        # This test hash is "frenchfries"
        test_hash = '$6$rounds=62763$MOB6eBAyRaFtrm0c$ETahVNQQujp2G0r2KrNYf4PNpI8/PcERpv/1w7evuCZC8n8OzBgCy6XVvjVxnUVNZlasdYMW3CShD/P4hBX.T/'

        if sins_context.verify(basicauth_pass, test_hash):
            stats = 'STATS:<br>'

            for qname in queues:
                stats += "Queue: %s <br> Current: %s <br> Listeners: %s<br>" % \
                         (qname, queues[qname].current, str(queues[qname].listeners))

            for sname in sessions:
                stats += "Session id: %s <br> Topics: %s <br>" %\
                         (sname, str(sessions[sname].topics))
        else:
            stats = "You didn't write the correct password, which is 'frenchfries' by the way."

        self.write(stats)

if __name__ == '__main__':
    application=tornado.web.Application([
        (r'/sins',SinsHandler),
        (r'/stats',StatsHandler),
    ])
    application.listen(5000)
    IOLoop.instance().start()
