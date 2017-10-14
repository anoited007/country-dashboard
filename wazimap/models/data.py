from collections import OrderedDict

from django.db import models
from django.utils.text import slugify
from django.contrib.postgres.fields import ArrayField

from itertools import groupby
from wazimap.data.base import Base
from wazimap.data.utils import get_session, capitalize, percent as p, add_metadata, current_context
from wazimap.data.tables import ZeroRow, INT_RE
from sqlalchemy import Column, String, Table, or_, and_, func
from sqlalchemy.orm import class_mapper
import sqlalchemy.types


class DataNotFound(Exception):
    pass


class Dataset(models.Model):
    """ Over-arching collection of data tables, spanning many releases.
    Such as a census that happens every decade. Two data tables from the
    same dataset and using the same universe, are comparable over time.
    """
    name = models.CharField(max_length=100, null=False, blank=False, unique=True, help_text="Friendly name of this dataset.")

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']


class Release(models.Model):
    name = models.CharField(max_length=100, null=False, blank=False, help_text="Name of this release, excluding the year.")
    year = models.CharField(max_length=50, null=False, blank=False, help_text="Primary year of this release. Will be used for sorting.")
    dataset = models.ForeignKey(Dataset, related_name='releases', null=False, on_delete=models.CASCADE)

    class Meta:
        unique_together = (('year', 'dataset'))
        ordering = ['name', 'year']

    def __str__(self):
        return '%s - %s' % (self.name, self.year)

    def as_dict(self):
        return {
            'name': self.name,
            'year': self.year,
        }


class DBTable(models.Model):
    # TODO: validator on name
    name = models.CharField(max_length=100, null=False, unique=True, blank=False, help_text="Name of the physical database table containing data for this DB table.")
    # Cache of SQLALchemy models for each db table
    MODELS = {}

    class Meta:
        ordering = ['name']

    def __init__(self, *args, **kwargs):
        super(DBTable, self).__init__(*args, **kwargs)
        self._model = None

    @property
    def model(self):
        if not self._model:
            self._model = DBTable.MODELS.get(self.name)
        # Could be None, in which case the caller must create a model
        return self._model

    @model.setter
    def model(self, model):
        DBTable.MODELS[self.name] = model
        self._model = model

    def __str__(self):
        return 'DBTable<%s>' % self.name


class DataTable(models.Model):
    NUMBER = 'number'
    PERC = 'percentage'
    CHOICES = (
        (NUMBER, NUMBER),
        (PERC, PERC)
    )

    name = models.SlugField(max_length=1024, null=False, blank=False, unique=True, help_text="Name for this table. No spaces.")
    universe = models.CharField(max_length=1024, null=False, blank=False, help_text="Universe this table samples from, such as 'Population', 'Households', or 'Youth aged 15-24'.")
    dataset = models.ForeignKey(Dataset, null=False, on_delete=models.CASCADE)
    stat_type = models.CharField(max_length=10, null=False, default=NUMBER, choices=CHOICES)
    description = models.CharField(max_length=1024, null=True, blank=True, help_text="Helpful description of this table (optional). Generated automatically for FieldTables if left blank.")

    release_class = None

    class Meta:
        abstract = True

    def clean(self):
        if not self.description:
            self.description = self._build_description()
        if self.name:
            self.name = self.name.upper()

    def get_release(self, year):
        """ Get the Release description for the specified year.
        """
        query = self.release_class.objects.filter(data_table=self)

        if year == 'latest':
            query = query.order_by('-release__year')
        else:
            query = query.filter(release__year=year)

        result = query.first()
        if result:
            return result.release

    def get_db_table(self, release=None, year=None):
        """ Get a DBTable instance for a particular year or release,
        or the latest if neither are specified.
        """
        if year is None and release is None:
            from wazimap.data.utils import current_context
            # use the current context
            year = current_context().get('year')

        if year:
            release = self.get_release(year)

        if not release:
            raise ValueError("Unclear which release year to use. Specify a release or a year, or use dataset_context(year=...)")

        # get the db_table
        fieldname = self.release_class.__name__.lower() + '__release'
        query = self.db_table_releases.filter(**{fieldname: release})

        db_table = query.first()
        db_table.active_release = release
        self.setup_model(db_table)

        return db_table

    def setup_model(self, db_table):
        pass

    def _build_description(self):
        pass

    def _build_model_columns(self):
        # We build this array in a particular order, with the geo-related fields first,
        # to ensure that SQLAlchemy creates the underlying table with the compound primary
        # key columns in the correct order:
        #
        #  geo_level, geo_code, geo_version, field, [field, field, ...]
        #
        # This means postgresql will use the first two elements of the compound primary
        # key -- geo_level and geo_code -- when looking up values for a particular
        # geograhy. This saves us from having to create a secondary index.
        columns = []

        # will form a compound primary key on the fields, and the geo id
        columns.append(Column('geo_level', String(15), nullable=False, primary_key=True))
        columns.append(Column('geo_code', String(10), nullable=False, primary_key=True))
        columns.append(Column('geo_version', String(100), nullable=False, primary_key=True, server_default=''))

        return columns

    def as_dict(self):
        return {
            'title': self.description or self.name,
            'universe': self.universe,
            'denominator_column_id': self.total_column,
            'table_id': self.name.upper(),
            'stat_type': self.stat_type,
            'releases': [r.as_dict() for r in self.releases()],
        }

    def releases(self):
        return list(set(r.release for r in self.release_class.objects
                        .filter(data_table=self)
                        .prefetch_related('release')
                        .all()))

    @classmethod
    def find(cls, name, universe=None, dataset=None):
        candidates = cls.objects.filter(name__iexact=name)
        if universe:
            candidates = candidates.filter(universe__iexact=universe)
        if dataset:
            candidates = candidates.filter(dataset__name__iexact=dataset)
        return candidates.first()


class SimpleTable(DataTable):
    total_column = models.CharField(max_length=50, null=True, help_text="Name of the column that contains the total value of all the columns in the row. Wazimap usse this to express column values as a percentage. If this is not set, the table doesn't have the concept of a total and only absolute values (not percentages) will be displayed.")
    db_table_releases = models.ManyToManyField(DBTable, through='SimpleTableRelease', through_fields=('data_table', 'db_table'))

    def __init__(self, *args, **kwargs):
        super(SimpleTable, self).__init__(*args, **kwargs)
        self.release_class = SimpleTableRelease

    def get_stat_data(self, geo, fields=None, key_order=None, percent=True, total=None, recode=None, year=None):
        """ Get a data dictionary for a place from this table.

        This fetches the values for each column in this table and returns a data
        dictionary for those values, with appropriate names and metadata.

        :param geo: the geography
        :param str or list fields: the columns to fetch stats for. By default, all columns except
                                   geo-related and the total column (if any) are used.
        :param str key_order: explicit ordering of (recoded) keys, or None for the default order.
                              Default order is the order in +fields+ if given, otherwise
                              it's the natural column order from the DB.
        :param bool percent: should we calculate percentages, or just include raw values?
        :param int total: the total value to use for percentages, name of a
                          field, or None to use the sum of all retrieved fields (default)
        :param dict recode: map from field names to strings to recode column names. Many fields
                            can be recoded to the same thing, their values will be summed.
        :param str year: release year to use. None will try to use the current dataset context, and 'latest'
                         will use the latest release.

        :return: (data-dictionary, total)
        """
        db_table = self.get_db_table(year=year or current_context().get('year'))
        model = db_table.model
        columns = self.columns(db_table)

        session = get_session()
        try:
            if fields is not None and not isinstance(fields, list):
                fields = [fields]
            if fields:
                for f in fields:
                    if f not in columns:
                        raise ValueError("Invalid field/column '%s' for table '%s'. Valid columns are: %s" % (
                            f, self.id, ', '.join(columns.keys())))
            else:
                fields = columns.keys()
                if self.total_column:
                    fields.remove(self.total_column)

            recode = recode or {}
            if recode:
                # change lambda to dicts
                if not isinstance(recode, dict):
                    recode = {f: recode(f) for f in fields}

            # is the total column valid?
            if isinstance(total, basestring) and total not in columns:
                raise ValueError("Total column '%s' isn't one of the columns for table '%s'. Valid columns are: %s" % (
                    total, self.id, ', '.join(columns.keys())))

            # table columns to fetch
            cols = [model.__table__.columns[c] for c in fields]

            if total is not None and isinstance(total, basestring) and total not in cols:
                cols.append(total)

            # do the query. If this returns no data, row is None
            row = session\
                .query(*cols)\
                .filter(model.geo_level == geo.geo_level,
                        model.geo_code == geo.geo_code,
                        model.geo_version == geo.version)\
                .first()

            if row is None:
                row = ZeroRow()

            # what's our denominator?
            if total is None:
                # sum of all columns
                total = sum(getattr(row, f) or 0 for f in fields)
            elif isinstance(total, basestring):
                total = getattr(row, total)

            # Now build a data dictionary based on the columns in +row+.
            # Multiple columns may be recoded into one, so we have to
            # accumulate values as we go.
            results = OrderedDict()

            key_order = key_order or fields  # default key order is just the list of fields

            for field in key_order:
                val = getattr(row, field) or 0

                # recode the key for this field, default is to keep it the same
                key = recode.get(field, field)

                # set the recoded field name, noting that the key may already
                # exist if another column recoded to it
                field_info = results.setdefault(key, {'name': recode.get(field, columns[field]['name'])})

                if percent:
                    # sum up existing values, if any
                    val = val + field_info.get('numerators', {}).get('this', 0)
                    field_info['values'] = {'this': p(val, total)}
                    field_info['numerators'] = {'this': val}
                else:
                    # sum up existing values, if any
                    val = val + field_info.get('values', {}).get('this', 0)
                    field_info['values'] = {'this': val}

            add_metadata(results, self, db_table.active_release)
            return results, total
        finally:
            session.close()

    def raw_data_for_geos(self, geos, release=None, year=None):
        # initial values
        data = {('%s-%s' % (geo.geo_level, geo.geo_code)): {
                'estimate': {},
                'error': {}}
                for geo in geos}

        db_table = self.get_db_table(release=release, year=year)
        columns = self.columns(db_table)

        session = get_session()
        try:
            geo_values = None
            rows = session\
                .query(db_table.model)\
                .filter(or_(and_(
                    db_table.model.geo_level == g.geo_level,
                    db_table.model.geo_code == g.geo_code,
                    db_table.model.geo_version == g.version)
                    for g in geos))\
                .all()

            for row in rows:
                geo_values = data['%s-%s' % (row.geo_level, row.geo_code)]

                for col in columns.iterkeys():
                    geo_values['estimate'][col] = getattr(row, col)
                    geo_values['error'][col] = 0

        finally:
            session.close()

        return data

    def columns(self, db_table=None, year=None, release=None):
        """ Work out our columns by finding those that aren't geo columns.
        """
        db_table = db_table or self.get_db_table(year=year, release=release)

        columns = OrderedDict()
        indent = 0
        if self.total_column:
            indent = 1

        for col in (c.name for c in db_table.model.__table__.columns if c.name not in ['geo_code', 'geo_level', 'geo_version']):
            columns[col] = {
                'name': capitalize(col.replace('_', ' ')),
                'indent': 0 if col == self.total_column else indent
            }

        # TODO: cache it?

        return columns

    def setup_model(self, db_table):
        model = db_table.model
        if not model:
            columns = self._build_model_columns()

            class Model(Base):
                __table__ = Table(db_table.name, Base.metadata, *columns, autoload=True, extend_existing=True)

            model = Model
            db_table.model = model

    def __str__(self):
        return self.name


class FieldTable(DataTable):
    INTEGER = 'Integer'
    FLOAT = 'Float'
    CHOICES = ((INTEGER, INTEGER), (FLOAT, FLOAT))

    fields = ArrayField(models.CharField(max_length=50, null=False, unique=True))
    db_table_releases = models.ManyToManyField(DBTable, through='FieldTableRelease', through_fields=('data_table', 'db_table'))
    denominator_key = models.CharField(max_length=50, null=True, blank=True,
                                       help_text='The key value of the rightmost field that should be used as the "total" column, ' +
                                                 'instead of summing over the values for each row. This is necessary when the ' +
                                                 'table doesn\'t describe a true partitioning of the dataset (ie. the row values ' +
                                                 'sum to more than the total population).  This will be used as the total column once ' +
                                                 'the id of the column has been calculated.')
    has_total = models.BooleanField(default=True, null=False,
                                    help_text="Does it make sense to calculate a total column and express percentages for values in this table?")
    value_type = models.CharField(max_length=20, null=False, blank=False, default=INTEGER, choices=CHOICES)

    def __init__(self, *args, **kwargs):
        super(FieldTable, self).__init__(*args, **kwargs)
        self.release_class = FieldTableRelease
        self._field_set = None
        if self.has_total:
            self.total_column = self.column_id([self.denominator_key or 'total'])
        else:
            self.total_column = None

    def clean(self):
        if not self.name:
            self.name = slugify(''.join(self.fields))

        super(FieldTable, self).clean()
        self._field_set = None

    @property
    def field_set(self):
        if self._field_set is None:
            self._field_set = set(self.fields)
        return self._field_set

    def setup_model(self, db_table):
        """ Build the model that corresponds to the table underlying this data table.
        """
        model = db_table.model
        if not model:
            columns = self._build_model_columns()

            # create the table model
            class Model(Base):
                __table__ = Table(db_table.name, Base.metadata, *columns, extend_existing=True)

            # ensure it exists in the DB
            session = get_session()
            try:
                Model.__table__.create(session.get_bind(), checkfirst=True)
            finally:
                session.close()

            db_table.model = Model

    def _build_model_columns(self):
        columns = super(FieldTable, self)._build_model_columns()
        value_type = getattr(sqlalchemy.types, self.value_type)

        # field columns
        columns.extend(Column(field, String(128), primary_key=True) for field in self.fields)
        # total column
        columns.append(Column('total', value_type, nullable=True))

        return columns

    def columns(self, db_table=None, year=None, release=None):
        """ Prepare a description of our columns for use by the data API.

        Each 'column' is actually a unique value for each of this table's +fields+.
        """
        # Each "column" is a unique permutation of the values
        # of this table's fields, including rollups. The ordering of the
        # columns is important since columns heirarchical, but are returned
        # "flat".
        #
        # Here's an example. Suppose our table has the following values:
        #
        #     5 years, male, 129
        #     5 years, female, 131
        #     10 years, male, 221
        #     10 years, female, 334
        #
        # This would produce the following columns (indented to show nesting)
        #
        # 5 years:
        #   male
        # 5 years:
        #   female
        # 10 years:
        #   male
        # 10 years:
        #   female

        # map from column id to column info.
        columns = OrderedDict()
        db_table = db_table or self.get_db_table(year=year, release=release)

        # TODO: cache this

        if self.has_total:
            columns[self.total_column] = {'name': 'Total', 'indent': 0}

        session = get_session()
        try:
            fields = [getattr(db_table.model, f) for f in self.fields]

            # get distinct permutations for all fields
            rows = session\
                .query(*fields)\
                .order_by(*fields)\
                .distinct()\
                .all()

            def permute(indent, field_values, rows):
                field = self.fields[indent - 1]
                last = indent == len(self.fields)

                for val, rows in groupby(rows, lambda r: getattr(r, field)):
                    # this is used to calculate the column id
                    new_values = field_values + [val]
                    col_id = self.column_id(new_values)

                    columns[col_id] = {
                        'name': capitalize(val) + ('' if last else ':'),
                        'indent': 0 if col_id == self.total_column else indent,
                    }

                    if not last:
                        permute(indent + 1, new_values, rows)

            permute(1, [], rows)
        finally:
            session.close()

        return columns

    def column_id(self, field_values):
        if len(field_values) == 1 and INT_RE.match(field_values[0]):
            # javascript re-orders keys that are pure integers, so force it to be a string
            return field_values[0] + "_"
        else:
            return '-'.join(field_values)

    def get_rows_for_geo(self, geo, session, fields=None, order_by=None, only=None, exclude=None, db_table=None):
        """ Get rows of statistics from the stats model +db_model+ for a particular
        geography, summing over the 'total' field and grouping by +fields+. Filters
        to include +only+ and ignore +exclude+, if given.
        """
        db_table = db_table or self.get_db_table()
        db_model = db_table.model

        if fields is None:
            fields = [c.key for c in class_mapper(db_model).attrs if c.key not in ['geo_code', 'geo_level', 'geo_version', 'total']]

        fields = [getattr(db_model, f) for f in fields]

        objects = session\
            .query(func.sum(db_model.total).label('total'), *fields)\
            .group_by(*fields)\
            .filter(db_model.geo_code == geo.geo_code)\
            .filter(db_model.geo_level == geo.geo_level)\
            .filter(db_model.geo_version == geo.version)

        if only:
            for k, v in only.iteritems():
                objects = objects.filter(getattr(db_model, k).in_(v))

        if exclude:
            for k, v in exclude.iteritems():
                objects = objects.filter(getattr(db_model, k).notin_(v))

        if order_by is not None:
            attr = order_by
            is_desc = False
            if order_by[0] == '-':
                is_desc = True
                attr = attr[1:]

            if attr == 'total':
                if is_desc:
                    attr = attr + ' DESC'
            else:
                attr = getattr(db_model, attr)
                if is_desc:
                    attr = attr.desc()

            objects = objects.order_by(attr)

        objects = objects.all()
        if len(objects) == 0:
            raise DataNotFound("Entry in %s for geography %s version '%s' not found"
                               % (db_table.name, geo.geoid, geo.version))
        return objects

    def raw_data_for_geos(self, geos, db_table=None):
        """ Pull raw data for a list of geo models.

        Returns a dict mapping the geo ids to table data.
        """
        # initial values
        data = {('%s-%s' % (geo.geo_level, geo.geo_code)): {
                'estimate': {},
                'error': {}}
                for geo in geos}

        db_table = db_table or self.get_db_table()

        session = get_session()
        try:
            geo_values = None
            fields = [getattr(db_table.model, f) for f in self.fields]
            rows = session\
                .query(db_table.model.geo_level,
                       db_table.model.geo_code,
                       func.sum(db_table.model.total).label('total'),
                       *fields)\
                .group_by(db_table.model.geo_level, db_table.model.geo_code, *fields)\
                .order_by(db_table.model.geo_level, db_table.model.geo_code, *fields)\
                .filter(or_(and_(
                    db_table.model.geo_level == geo.geo_level,
                    db_table.model.geo_code == geo.geo_code,
                    db_table.model.geo_version == geo.version)
                    for geo in geos))\
                .all()

            def permute(level, field_keys, rows):
                field = self.fields[level]
                total = None
                denominator = 0

                for key, rows in groupby(rows, lambda r: getattr(r, field)):
                    new_keys = field_keys + [key]
                    col_id = self.column_id(new_keys)

                    if level + 1 < len(self.fields):
                        value = permute(level + 1, new_keys, rows)
                    else:
                        # we've bottomed out

                        rows = list(rows)
                        if all(row.total is None for row in rows):
                            value = None
                        else:
                            value = sum(row.total or 0 for row in rows)

                        if self.denominator_key and self.denominator_key == key:
                            # this row must be used as the denominator total,
                            # rather than as an entry in the table
                            denominator = value
                            continue

                    if value is not None:
                        total = (total or 0) + value
                    geo_values['estimate'][col_id] = value
                    geo_values['error'][col_id] = 0

                if self.denominator_key:
                    total = denominator

                return total

            # rows for each geo
            for geo_id, geo_rows in groupby(rows, lambda r: (r.geo_level, r.geo_code)):
                geo_values = data['%s-%s' % geo_id]
                total = permute(0, [], geo_rows)

                # total
                if self.total_column:
                    geo_values['estimate'][self.total_column] = total
                    geo_values['error'][self.total_column] = 0

        finally:
            session.close()

        return data

    def _build_description(self):
        return self.universe + ' by ' + ', '.join(self.fields)

    def __str__(self):
        return ', '.join(self.fields)

    @classmethod
    def for_fields(cls, fields, universe=None, dataset=None):
        """ Lookup a FieldTable that is suitable for a set of fields.

        If there are multiple tables that support these fields, the one with
        the least number of additional different fields is use.
        """
        # try find it based on fields
        field_set = set(fields)

        candidates = cls.objects.filter(fields__contains=list(field_set))
        if universe:
            candidates = candidates.filter(universe=universe)
        if dataset:
            candidates = candidates.filter(dataset__name=dataset)

        possibilities = [
            (t, len(t.field_set - field_set))
            for t in candidates if len(t.field_set) >= len(field_set) and len(field_set - t.field_set) == 0]
        table, _ = min(possibilities, key=lambda p: p[1])

        return table


class SimpleTableRelease(models.Model):
    data_table = models.ForeignKey(SimpleTable, on_delete=models.CASCADE)
    db_table = models.ForeignKey(DBTable, on_delete=models.CASCADE)
    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    def __str__(self):
        return '%s for %s in %s' % (self.db_table, self.data_table, self.release)


class FieldTableRelease(models.Model):
    data_table = models.ForeignKey(FieldTable, on_delete=models.CASCADE)
    db_table = models.ForeignKey(DBTable, on_delete=models.CASCADE)
    release = models.ForeignKey(Release, on_delete=models.CASCADE)

    def __str__(self):
        return '%s for %s in %s' % (self.db_table, self.data_table, self.release)