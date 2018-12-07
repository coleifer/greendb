import datetime
import struct
import sys
import time

from greendb import Client


__all__ = [
    'Client',
    'Field',
    'IntegerField',
    'LongField',
    'DateTimeField',
    'TimestampField',
    'BooleanField',
    'Model',
]


if sys.version_info[0] == 2:
    unicode_type = unicode
else:
    unicode_type = str


class Node(object):
    def __init__(self):
        self.negated = False

    def _e(op, inv=False):
        """
        Lightweight factory which returns a method that builds an Expression
        consisting of the left-hand and right-hand operands, using `op`.
        """
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner

    __and__ = _e('AND')
    __or__ = _e('OR')
    __rand__ = _e('AND', inv=True)
    __ror__ = _e('OR', inv=True)
    __eq__ = _e('=')
    __ne__ = _e('!=')
    __lt__ = _e('<')
    __le__ = _e('<=')
    __gt__ = _e('>')
    __ge__ = _e('>=')

    def between(self, start, stop, start_inclusive=True, stop_inclusive=False):
        return Expression(self, 'between', (start, stop, start_inclusive,
                                            stop_inclusive))

    def startswith(self, prefix):
        return Expression(self, 'startswith', prefix)


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __repr__(self):
        return '<Expression: %s %s %s>' % (self.lhs, self.op, self.rhs)


def encode(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, unicode_type):
        return s.encode('utf8')
    return str(s).encode('utf8')


class Field(Node):
    _counter = 0

    def __init__(self, index=False, default=None):
        self.index = index
        self.default = default
        self.model = None
        self.name = None
        self._order = Field._counter
        Field._counter += 1

    def __repr__(self):
        return '<%s: %s.%s>' % (
            type(self),
            self.model._meta.model_name,
            self.name)

    def bind(self, model, name):
        self.model = model
        self.name = name
        setattr(self.model, self.name, FieldDescriptor(self))

    def clone(self):
        field = type(self)(index=self.index, default=self.default)
        field.model = self.model
        field.name = self.name
        return field

    def serialize(self, value):
        return value

    def deserialize(self, value):
        return value

    def index_value(self, value):
        return encode(value)


class IntegerField(Field):
    def index_value(self, value):
        return struct.pack('>H', value)


class LongField(Field):
    def index_value(self, value):
        return struct.pack('>Q', value)


class DateTimeField(Field):
    def serialize(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f')

    def deserialize(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')

    def index_value(self, value):
        timestamp = time.mktime(value.timetuple())
        timestamp += value.microsecond * .000001
        timestamp = int(timestamp * 1e6)
        return struct.pack('>Q', timestamp)


class TimestampField(Field):
    def __init__(self, index=False, default=datetime.datetime.now):
        super(TimestampField, self).__init__(index=index, default=default)

    def serialize(self, value):
        timestamp = time.mktime(value.timetuple())
        timestamp += value.microsecond * .000001
        timestamp = int(timestamp * 1e6)
        return struct.pack('>Q', timestamp)

    def deserialize(self, value):
        raw_ts, = struct.unpack('>Q', value)
        timestamp, micro = divmod(raw_ts, 1e6)
        return (datetime.datetime
                .fromtimestamp(timestamp)
                .replace(microsecond=int(micro)))

    index_value = serialize


class BooleanField(Field):
    def index_value(self, value):
        return b'\x01' if value else b'\x00'


class FieldDescriptor(object):
    def __init__(self, field):
        self.field = field
        self.name = self.field.name

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance._data.get(self.name)
        return self.field

    def __set__(self, instance, value):
        instance._data[self.name] = value


class DeclarativeMeta(type):
    def __new__(cls, name, bases, attrs):
        if bases == (object,):
            return super(DeclarativeMeta, cls).__new__(cls, name, bases, attrs)

        client = None
        db = None
        index_db = None
        fields = {}

        # Inherit fields from parent classes.
        for base in bases:
            if not hasattr(base, '_meta'):
                continue

            for field in base._meta.sorted_fields:
                if field.name not in fields:
                    fields[field.name] = field.clone()

            if client is None and base._meta.client is not None:
                client = base._meta.client
            if db is None:
                db = base._meta.db
            if index_db is None:
                index_db = base._meta.index_db

        # Apply defaults if no value was found.
        db = 0 if db is None else db
        index_db = 15 if index_db is None else index_db

        # Introspect all declared fields.
        for key, value in attrs.items():
            if isinstance(value, Field):
                fields[key] = value

        # Read metadata configuration.
        declared_meta = attrs.pop('Meta', None)
        if declared_meta:
            if getattr(declared_meta, 'client', None) is not None:
                client = declared_meta.client
            if getattr(declared_meta, 'db', None) is not None:
                db = declared_meta.db
            if getattr(declared_meta, 'index_db', None) is not None:
                index_db = declared_meta.index_db

        # Always have an `id` field.
        if 'id' not in fields:
            fields['id'] = LongField()

        attrs['_meta'] = Metadata(name, client, db, index_db, fields)
        model = super(DeclarativeMeta, cls).__new__(cls, name, bases, attrs)

        # Bind fields to model.
        for name, field in fields.items():
            field.bind(model, name)

        # Process
        model._meta.prepared()

        return model


class Metadata(object):
    def __init__(self, model_name, client, db, index_db, fields):
        self.model_name = model_name
        self.client = client
        self.db = db
        self.index_db = index_db
        self.fields = fields

        self.name = model_name.lower()
        self.sequence = 'id_seq:%s' % self.name

        self.defaults = {}
        self.defaults_callable = {}

    def set_client(self, client):
        self.client = client

    def prepared(self):
        self.sorted_fields = sorted(
            [field for field in self.fields.values()],
            key=lambda field: field._order)

        # Populate index attributes.
        self.indexed_fields = set()
        self.indexed_field_objects = []
        self.indexes = {}
        for field in self.sorted_fields:
            if field.index:
                self.indexed_fields.add(field.name)
                self.indexed_field_objects.append(field)
                self.indexes[field.name] = Index(self.client, self.index_db,
                                                 field)

        for field in self.sorted_fields:
            if callable(field.default):
                self.defaults_callable[field.name] = field.default
            elif field.default:
                self.defaults[field.name] = field.default

    def next_id(self):
        return self.client.incr(self.sequence)

    def get_instance_key(self, instance_id):
        return '%s:%s' % (self.name, instance_id)


def with_metaclass(meta, base=object):
    return meta('newbase', (base,), {})


class Model(with_metaclass(DeclarativeMeta)):
    def __init__(self, **kwargs):
        self._data = self._meta.defaults.copy()
        for key, value in self._meta.defaults_callable.items():
            self._data[key] = value()
        self._data.update(kwargs)

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def load(cls, primary_key):
        data = cls._read_model_data(primary_key)
        if data is None:
            raise KeyError('%s with id=%s not found' %
                           (cls._meta.name, primary_key))
        return cls(**data)

    def save(self):
        if self.id and self._meta.indexes:
            original_data = self._read_model_data(primary_key)
        else:
            original_data = None

        # Save the actual model data.
        self._save_model_data()

        # Update any secondary indexes.
        self._update_indexes(original_data)

    def _save_model_data(self):
        # Generate the next ID in sequence if no ID is set.
        if not self.id:
            self.id = self._meta.next_id()

        # Retrieve the primary key identifying this model instance.
        key = self._meta.get_instance_key(self.id)

        # Prepare the data for storage. Some Python data-types, e.g. datetimes,
        # cannot be serialized natively by the greendb protocol, so we
        # serialize them before writing.
        accum = {}
        for field in self._meta.sorted_fields:
            value = self._data.get(field.name)
            if value is not None:
                accum[field.name] = field.serialize(value)

        self._meta.client.set(key, accum)  # Alt: .set(key, self._data).

    def _update_indexes(self, original_data=None):
        primary_key = self.id

        # Store current model data in the index.
        for field_name, index in self._meta.indexes.items():
            field = self._meta.fields[field_name]
            new_value = self._data.get(field_name)
            if original_data is not None and field_name in original_data:
                old_value = original_data[field_name]
                if old_value != new_value:
                    index.delete(old_value, primary_key)

            if new_value is not None:
                index.store(new_value, primary_key)

    @classmethod
    def _read_model_data(cls, primary_key):
        key = cls._meta.get_instance_key(primary_key)
        data = cls._meta.client.get(key)
        if data is not None:
            return cls._deserialize_raw_data(data)

    @classmethod
    def _deserialize_raw_data(cls, data):
        accum = {}
        for key, value in data.items():
            field = cls._meta.fields.get(key)
            if field is not None:
                accum[key] = field.deserialize(value)
            else:
                accum[key] = value
        return accum

    def delete(self):
        primary_key = self.id

        # Load up original data if we have indexes to clean up.
        if self._meta.indexes:
            data = self._read_model_data(primary_key)
        else:
            data = None

        # Delete the model data.
        key = self._meta.get_instance_key(primary_key)
        self._meta.client.delete(key)

        # Clear out the indexes.
        if data is not None:
            for field_name, index in self._meta.indexes.items():
                value = data.get(field_name) or self._data.get(field_name)
                if value is not None:
                    field = self._meta.fields[field_name]
                    index.delete(value, self.id)

    @classmethod
    def get(cls, expr):
        results = cls.query(expr)
        if results:
            return results[0]

    @classmethod
    def all(cls, count=None):
        accum = []
        prefix = encode(cls._meta.name)
        start = prefix + b':\x00'
        stop = prefix + b':\xff'
        for k, v in cls._meta.client.getrange(start, stop, count):
            data = cls._deserialize_raw_data(v)
            accum.append(cls(**data))
        return accum

    @classmethod
    def query(cls, expr):
        def dfs(expr):
            lhs = expr.lhs
            rhs = expr.rhs
            if isinstance(lhs, Expression):
                lhs = dfs(lhs)
            if isinstance(rhs, Expression):
                rhs = dfs(rhs)

            if isinstance(lhs, Field):
                index = cls._meta.indexes[lhs.name]
                return index.query(rhs, expr.op)
            elif expr.op == 'AND':
                return set(lhs) & set(rhs)
            elif expr.op == 'OR':
                return set(lhs) | set(rhs)
            else:
                raise ValueError('Unable to execute query, unexpected type.')

        # Collect raw list of integer IDs.
        id_list = dfs(expr)

        # Bulk-load the model data, creating a mapping of model key -> int id.
        keys = [encode(cls._meta.get_instance_key(pk)) for pk in id_list]
        key_to_data = cls._meta.client.mget(keys)

        deserialize = cls._deserialize_raw_data
        return [cls(**deserialize(key_to_data[key])) for key in keys]


class Index(object):
    def __init__(self, client, index_db, field):
        self.client = client
        self.db = index_db
        self.field = field
        self.key = 'idx:%s:%s' % (field.model._meta.name, field.name)

        # Obtain references to serialization routine for query values, e.g.
        # datetimes are stored as 64-bit unsigned integer microseconds. So when
        # we receive a datetime, we need to convert the incoming user value.
        self.convert = field.index_value

    def get_value(self, value, primary_key):
        return b'%s\x00%s' % (self.convert(value), encode(primary_key))

    def store(self, value, primary_key):
        value = self.get_value(value, primary_key)
        return self.client.setdupraw(self.key, value, db=self.db)

    def delete(self, value, primary_key):
        value = self.get_value(value, primary_key)
        return self.client.deletedupraw(self.key, value, db=self.db)

    def _range_query(self, start=None, stop=None):
        return self.client.getrangedupraw(self.key, start, stop, db=self.db)

    def get_range_values(self, start=None, stop=None):
        accum = []
        for raw_value in self._range_query(start, stop):
            delim_idx = raw_value.rfind(b'\x00')
            accum.append(int(raw_value[delim_idx + 1:].decode('ascii')))
        return accum

    def query(self, value, operation):
        if operation == '=':
            bval = self.convert(value)
            return self.get_range_values(bval + b'\x00', bval + b'\x00\xff')
        elif operation in ('<', '<='):
            bval = self.convert(value)
            stop = bval + (b'\x00\x00' if operation == '<' else b'\x00\xff')
            return self.get_range_values(None, stop)
        elif operation in ('>', '>='):
            bval = self.convert(value)
            start = bval + (b'\x00\xff' if operation == '>' else b'\x00\x00')
            return self.get_range_values(start)
        elif operation == 'between':
            sstart, sstop, start_incl, stop_incl = value
            start = self.convert(sstart)
            stop = self.convert(sstop)
            start = start + (b'\x00\x00' if start_incl else b'\x00\xff')
            stop = stop + (b'\x00\xff' if stop_incl else b'\x00\x00')
            return self.get_range_values(start, stop)
        elif operation == '!=':
            bval = self.convert(value)
            accum = []
            for raw_value in self._range_query():
                idx_value, pk = raw_value.rsplit(b'\x00', 1)
                if bval != idx_value:
                    accum.append(int(pk.decode('ascii')))
            return accum
        elif operation == 'startswith':
            bval = self.convert(value)
            return self.get_range_values(bval, bval + b'\xff')
        else:
            raise ValueError('unrecognized operation: "%s"' % operation)
