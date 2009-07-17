
import re, random
from hashlib import md5
from base64 import b64encode

from django.http import HttpResponse
from analytics.django import analytics

def decode_attribute(name, value):
    if '.' not in name:
        typ = 's'
    else:
        name, typ = name.rsplit('.', 1)
    if typ == 's':
        value = value.decode('utf-8')
    elif typ == 'i':
        value = int(value)
    elif typ == 'f':
        value = float(value)
    elif typ == 'b':
        value = bool(int(value))
    # TODO:
    #   duration = 'd',
    #   timestamp = 't',
    #   url = 'u',
    #   tags = 'k',
    #   ip_address = 'a',
    return (str(name), value)

VALID_ATTRIBUTE_NAME_RE = re.compile(r"^[a-z_]+(\.[sifbdtuka])?$")

def is_valid_name(name):
    if name in ('key', 'event'):
        return False
    return VALID_ATTRIBUTE_NAME_RE.match(name)

def record(request):
    key = request.GET['key']
    event = request.GET['event']
    attributes = dict(decode_attribute(k, v) for k, v in request.GET.iteritems() if is_valid_name(k))

    if 'user_id' in attributes:
        visit_id = b64encode(md5(str(attributes['user_id'])))
    else:
        visit_id = request.COOKIES.get('vid')
        if not visit_id:
            visit_id = b64encode(md5(str(random.random())).digest())

    attributes['visit_id'] = visit_id

    analytics.record(event, **attributes)

    response = HttpResponse("{}", mimetype="application/json")
    response.set_cookie('vid', visit_id)
    return response
