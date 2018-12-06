import datetime
import struct

from greendb import Client


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
            self.model._meta.name,
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

    def db_value(self, value):
        return value

    def python_value(self, value):
        return value


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
            if db is None and base._meta.db is not None:
                db = base._meta.db
            if index_db is None and base._meta.index_db is not None:
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
            fields['id'] = Field()

        attrs['_meta'] = Metadata(name, client, db, index_db, fields)
        model = super(DeclarativeMeta, cls).__new__(cls, name, bases, attrs)

        # Bind fields to model.
        for name, field in fields.items():
            field.bind(model, name)

        # Process
        model._meta.prepared()

        return model


class IndexClient(object):
    def __init__(self, client, db, index_db):
        self.client = client
        self.db = db
        self.index_db = index_db

    def __enter__(self):
        self.client.use(self.index_db)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.use(self.db)


class Metadata(object):
    def __init__(self, model_name, client, db, index_db, fields):
        self.model_name = model_name
        self.client = client
        self.db = db
        self.index_db = index_db
        self.index_client = IndexClient(client, db, index_db)
        self.fields = fields

        self.name = model_name.lower()
        self.sequence = 'id_seq:%s' % self.name

        self.defaults = {}
        self.defaults_callable = {}

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
                self.indexes[field.name] = Index(self.client, field)

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
        return cls(**cls._read_model_data(primary_key))

    def save(self):
        if self.id and self._meta.indexes:
            original_data = self._data
        else:
            original_data = None

        # Save the actual model data.
        self._save_model_data()

        # Update any secondary indexes.
        with self._meta.index_client:
            self._update_indexes(original_data)

    def _save_model_data(self):
        # Generate the next ID in sequence if no ID is set.
        if not self.id:
            self.id = self._meta.next_id()

        # Retrieve the primary key identifying this model instance.
        key = self._meta.get_instance_key(self.id)

        # Store all model data serialized in a single record.
        self._meta.client.set(key, self._data)

    def _update_indexes(self, original_data):
        primary_key = self.id
        indexes = self._meta.indexes.items()

        # If we are updating a row and the instance value differs from what was
        # previously stored, remove the old value.
        if original_data is not None:
            for field, index in indexes:
                if field in original_data:
                    value = getattr(self, field)
                    if original_data[field] != value:
                        index.delete(value, primary_key)

        # Store current model data in the index.
        for field, index in indexes:
            index.store(getattr(self, field), primary_key)

    @classmethod
    def _read_model_data(cls, primary_key, fields=None):
        key = cls._meta.get_instance_key(primary_key)
        return cls._meta.client.get(key)

    def delete(self, atomic=True):
        key = self._meta.get_instance_key(self.id)
        self._meta.client.delete(key)

        with self._meta.index_client:
            for field, index in self._meta.indexes.items():
                index.delete(getattr(self, field), self.id)

    @classmethod
    def get(cls, expr):
        results = cls.query(expr)
        if results:
            return results[0]

    @classmethod
    def all(cls, count=None):
        accum = []
        start = cls._meta.name + ':\x00'
        stop = cls._meta.name + ':\xff'
        for k, v in cls._meta.client.getrange(start, stop, count):
            accum.append(cls(**v))
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

        with cls._meta.index_client:
            id_list = dfs(expr)

        return [cls.load(primary_key) for primary_key in id_list]


class Index(object):
    def __init__(self, client, field):
        self.client = client
        self.field = field
        self.key = 'idx:%s:%s' % (field.model._meta.name, field.name)
        self.stop_key = self.key + b'\xff'
        self.encode_pk = field.model.id.db_value
        self.decode_pk = field.model.id.python_value

    def get_value(self, value, primary_key):
        return '%s\x00%s' % (
            self.field.db_value(value) or '',
            self.encode_pk(primary_key))

    def store(self, value, primary_key):
        self.client.setdupraw(self.key, self.get_value(value, primary_key))

    def delete(self, value, primary_key):
        self.client.deletedupraw(self.key, self.get_value(value, primary_key))

    def _range_query(self, start=None, stop=None):
        return self.client.getrangedupraw(self.key, start, stop)

    def get_range_values(self, start=None, stop=None):
        accum = []
        for raw_value in self.client.getrangedupraw(self.key, start, stop):
            delim_idx = raw_value.rfind(b'\x00')
            accum.append(self.decode_pk(raw_value[delim_idx + 1:]))
        return accum

    def query(self, value, operation):
        if operation == '=':
            # Equality means range from value + delim, to value and any pk.
            start = value + '\x00'
            stop = value + '\x00\xff'
            return self.get_range_values(start, stop)
        elif operation in ('<', '<='):
            stop = value + ('\x00\x00' if operation == '<' else '\x00\xff')
            return self.get_range_values(None, stop)
        elif operation in ('>', '>='):
            start = value + ('\x00\xff' if operation == '>' else '\x00\x00')
            return self.get_range_values(start)
        elif operation == 'between':
            start, stop, start_incl, stop_incl = value
            start = start + ('\x00\x00' if start_incl else '\x00\xff')
            stop = stop + ('\x00\xff' if stop_incl else '\x00\x00')
            return self.get_range_values(start, stop)
        elif operation == '!=':
            for raw_value in self._range_query():
                idx_value, pk = raw_value.rsplit(':', 1)
                if value != idx_value:
                    accum.append(self.decode_pk(pk))
            return accum
        elif operation == 'startswith':
            return self.get_range_values(value, value + '\xff')
        else:
            raise ValueError('unrecognized operation: "%s"' % operation)


if __name__ == '__main__':
    client = Client()
    client.flushall()

    class User(Model):
        username = Field(index=True)
        status = Field(index=True)

        class Meta:
            client = client

    username_status = (
        ('huey', 1),
        ('mickey', 0),
        ('zaizee', 1),
        ('beanie', 2),
        ('gracie', 0),
        ('rocky', 0),
        ('rocky-2', 0),
        ('rocky-3', 0),
    )
    for username, status in username_status:
        User.create(username=username, status=status)

    u_db = User.load(1)
    print(u_db.username)
    print(u_db._data)

    print('All users')
    for user in User.all():
        print(user.id, user.username, user.status)

    print('\nUsers between "huey" and "rocky" (inclusive)')
    query = User.query(User.username.between('huey', 'rocky', True, True))
    for user in query:
        print(user.id, user.username, user.status)

    print('\nUsers with status >= 1')
    for user in User.query(User.status >= '1'):
        print(user.id, user.username, user.status)

    print('\nKeys')
    print(client.keys())
    with User._meta.index_client:
        print(client.keys())
    client.flushall()
