from __future__ import annotations
from dataclasses import dataclass, field
from functools import total_ordering
from typing import overload
from enum import Enum, auto
import datetime
import zoneinfo
import tzlocal
import sys



@dataclass(frozen=True)
@total_ordering
class Timestamp:
    '''Local date and time along with the time zone.

    Contains an aware datetime timestamp with a 'ZoneInfo' time zone.

    Requirements:
    - 'tzinfo' must be 'zoneinfo.ZoneInfo'.

    Notes:
    - This class strictly stores 'tzinfo' as 'zoneinfo.ZoneInfo'
      instances (IANA names).
    - Dependency: 'tzlocal' (for detecting local IANA zone name).'''


    _UTC = zoneinfo.ZoneInfo('Etc/UTC')    # UTC time zone.


    _dt: datetime.datetime
    

    @staticmethod
    def _is_valid_dt(dt: datetime.datetime) -> bool:
        '''Checks that the 'datetime' variable contains a valid
        'ZoneInfo' time zone.'''

        if dt.tzinfo is None:
            return False
        try:
            utc_off = dt.tzinfo.utcoffset(dt)
        except Exception:
            return False
        return utc_off is not None and isinstance(dt.tzinfo, zoneinfo.ZoneInfo)


    def __post_init__(self) -> None:
        if not Timestamp._is_valid_dt(self._dt):
            raise ValueError('The time zone has been set incorrectly.')
        
    
    def __str__(self) -> str:
        return f'{self.datetime_iso}, {self.timezone_iana}'
    

    def __eq__(self, other: object) -> bool:
        '''Compares two timestamps in UTC.'''

        if not isinstance(other, Timestamp):
            return NotImplemented
        
        return self._dt.astimezone(Timestamp._UTC) == other._dt.astimezone(Timestamp._UTC)
    

    def __hash__(self) -> int:
        dt_utc = self._dt.astimezone(Timestamp._UTC)
        return hash(dt_utc)
    

    def __lt__(self, other: object) -> bool:
        '''Less-than comparison based on absolute (UTC) time.'''
    
        if not isinstance(other, Timestamp):
            return NotImplemented
        
        return self._dt.astimezone(Timestamp._UTC) < other._dt.astimezone(Timestamp._UTC)
    

    def __add__(self, other: datetime.timedelta) -> Timestamp:
        '''Time shift by a specified interval.'''

        if not isinstance(other, datetime.timedelta):
            return NotImplemented

        tz = self._dt.tzinfo
        dt_utc = self._dt.astimezone(Timestamp._UTC)
        dt_utc_new = dt_utc + other
        dt_new = dt_utc_new.astimezone(tz)
        return Timestamp(dt_new)
    

    @overload
    def __sub__(self, other: datetime.timedelta) -> Timestamp: ...


    @overload
    def __sub__(self, other: Timestamp) -> datetime.timedelta: ...
    

    def __sub__(self, other: datetime.timedelta | Timestamp) -> Timestamp | datetime.timedelta:
        '''The offset of a timestamp by a specified interval,
        or the difference between two timestamps.'''

        if isinstance(other, datetime.timedelta):
            return self + (-other)

        if isinstance(other, Timestamp):
            self_dt_utc = self._dt.astimezone(Timestamp._UTC)
            other_dt_utc = other._dt.astimezone(Timestamp._UTC)
            return self_dt_utc - other_dt_utc    # 'timedelta'.

        return NotImplemented
    

    @classmethod
    def from_utc(cls, dt_iso: str) -> Timestamp:
        '''Parse an ISO 8601 string and return a 'Timestamp' in UTC.

        Any strings containing timestamps with a non-zero offset are rejected.

        Accepted examples:
         - '2026-01-20T10:36'         (assumed UTC),
         - '2026-01-20T10:36Z'        (UTC),
         - '2026-01-20T10:36+00:00'   (zero offset).'''
        
        # Manually replace the suffix 'Z' with zero offset '+00:00'
        # for older versions of Python (< 3.11).
        if sys.version_info < (3, 11) and dt_iso.endswith('Z'):
            dt_iso = dt_iso[:-1] + '+00:00'
        
        try:
            dt = datetime.datetime.fromisoformat(dt_iso)
        except ValueError as e:
            raise ValueError(f'Invalid ISO 8601 datetime string: {dt_iso}') from e

        if dt.tzinfo is None:
            # Naive: interpret as UTC per method contract.
            dt = dt.replace(tzinfo=Timestamp._UTC)
        else:
            # Ensure the moment is UTC (offset zero).
            if dt.utcoffset() != datetime.timedelta(0):
                raise ValueError(f"The timestamp '{dt_iso}' is not in UTC.")
            # Convert to 'ZoneInfo('Etc/UTC')' to satisfy strict storage
            # invariant.
            dt = dt.astimezone(Timestamp._UTC)

        return cls(dt)
    

    @classmethod
    def now(cls) -> Timestamp:
        '''Creates a new timestamp with the current local time.'''

        try:
            tz = zoneinfo.ZoneInfo(tzlocal.get_localzone_name())
            return cls(datetime.datetime.now(tz))
        except Exception as e:
            raise RuntimeError(
                'Failed to determine local time zone.'
            ) from e
    

    @classmethod
    def now_utc(cls) -> Timestamp:
        '''Creates a new timestamp with the current time in UTC.'''
        
        try:
            return cls(datetime.datetime.now(Timestamp._UTC))
        except Exception as e:
            raise RuntimeError('Failed to determine UTC time.') from e
    

    @property
    def datetime(self) -> datetime.datetime:
        return self._dt
    

    @property
    def datetime_iso(self) -> str:
        '''Returns the timestamp in ISO 8601 format in the time zone
        in which it was recorded.'''
        
        return self._dt.isoformat()
    

    @property
    def timezone_iana(self) -> str:
        '''Returns the time zone of this timestamp in IANA format.'''

        if not isinstance(self._dt.tzinfo, zoneinfo.ZoneInfo):
            raise ValueError('The time zone has been set incorrectly.')

        return self._dt.tzinfo.key
    

    def to_timezone(self, timezone_iana: str) -> Timestamp:
        '''Creates a new timestamp by converting the given one to
        the specified time zone.'''

        try:
            tz = zoneinfo.ZoneInfo(timezone_iana)
        except Exception as e:
            # 'ZoneInfo' raises 'ZoneInfoNotFoundError' (subclass
            # of Exception) on bad names.
            raise ValueError(f'Invalid IANA time zone: {timezone_iana}') from e

        dt = self._dt.astimezone(tz)
        return Timestamp(dt)


    def to_utc(self) -> Timestamp:
        '''Creates a new timestamp by converting the given one
        to UTC.'''
        
        dt_utc = self._dt.astimezone(Timestamp._UTC)
        return Timestamp(dt_utc)
    

    @property
    def utc_iso(self) -> str:
        '''Returns an ISO 8601 string in UTC.'''
    
        dt_utc = self._dt.astimezone(Timestamp._UTC)
        return dt_utc.replace(tzinfo=None).isoformat() + 'Z'



@dataclass(frozen=True)
class TimeInterval:
    '''A time interval. Represents a connected subset of the time axis.

    This can be an empty set, a point, an open interval,
    a half-open interval, a closed interval, an open or closed ray,
    or the entire timeline.'''


    class Kind(Enum):
        '''Specifies the mathematical type of the interval.'''

        EMPTY = auto()
        POINT = auto()
        OPEN = auto()
        CLOSED = auto()    # Not a point.
        CLOSED_OPEN = auto()
        OPEN_CLOSED = auto()
        RIGHT_OPEN = auto()
        RIGHT_CLOSED = auto()
        LEFT_OPEN = auto()
        LEFT_CLOSED = auto()
        TIMELINE = auto()
    

    _BOUNDED_KINDS = {
        Kind.EMPTY,
        Kind.POINT,
        Kind.OPEN,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.OPEN_CLOSED,
    }

    _LEFT_BOUNDED_KINDS = {
        Kind.EMPTY,
        Kind.POINT,
        Kind.OPEN,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.OPEN_CLOSED,
        Kind.RIGHT_OPEN,
        Kind.RIGHT_CLOSED
    }

    _RIGHT_BOUNDED_KINDS = {
        Kind.EMPTY,
        Kind.POINT,
        Kind.OPEN,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.OPEN_CLOSED,
        Kind.LEFT_OPEN,
        Kind.LEFT_CLOSED
    }

    _OPEN_KINDS = {
        Kind.EMPTY,
        Kind.OPEN,
        Kind.RIGHT_OPEN,
        Kind.LEFT_OPEN,
        Kind.TIMELINE
    }

    # In a mathematical sense, non-openness does not mean closedness.
    _CLOSED_KINDS = {
        Kind.EMPTY,
        Kind.POINT,
        Kind.CLOSED,
        Kind.RIGHT_CLOSED,
        Kind.LEFT_CLOSED,
        Kind.TIMELINE
    }

    _START_SPECIFIED_KINDS = {
        Kind.POINT,
        Kind.OPEN,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.OPEN_CLOSED,
        Kind.RIGHT_OPEN,
        Kind.RIGHT_CLOSED
    }

    _END_SPECIFIED_KINDS = {
        Kind.POINT,
        Kind.OPEN,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.OPEN_CLOSED,
        Kind.LEFT_OPEN,
        Kind.LEFT_CLOSED
    }

    _START_INCLUDED_KINDS = {
        Kind.POINT,
        Kind.CLOSED,
        Kind.CLOSED_OPEN,
        Kind.RIGHT_CLOSED
    }

    _END_INCLUDED_KINDS = {
        Kind.POINT,
        Kind.CLOSED,
        Kind.OPEN_CLOSED,
        Kind.LEFT_CLOSED
    }


    _kind: Kind = Kind.EMPTY
    _start: Timestamp | None = None
    _end: Timestamp | None = None


    def _is_valid(self) -> bool:
        '''Checks whether the time interval is set correctly.'''

        match self._kind:
            case TimeInterval.Kind.EMPTY:
                return self._start is None and self._end is None

            case TimeInterval.Kind.POINT:
                return (
                    self._start is not None
                    and self._end is not None
                    and self._start == self._end
                )

            case (
                TimeInterval.Kind.OPEN |
                TimeInterval.Kind.CLOSED |    # Not a point.
                TimeInterval.Kind.CLOSED_OPEN |
                TimeInterval.Kind.OPEN_CLOSED
            ):
                return (
                    self._start is not None
                    and self._end is not None
                    and self._start < self._end
                )
            
            case TimeInterval.Kind.RIGHT_OPEN | TimeInterval.Kind.RIGHT_CLOSED:
                return self._start is not None and self._end is None
            
            case TimeInterval.Kind.LEFT_OPEN | TimeInterval.Kind.LEFT_CLOSED:
                return self._start is None and self._end is not None

            case TimeInterval.Kind.TIMELINE:
                return self._start is None and self._end is None

        return False

    
    def __post_init__(self) -> None:
        if not self._is_valid():
            raise ValueError('The time interval has been set incorrectly.')
        

    def __str__(self) -> str:

        match self._kind:
            case TimeInterval.Kind.EMPTY:
                # Returns the empty set symbol. 
                return '\u2205'

            case TimeInterval.Kind.POINT:
                return f'{{{self._start}}}'

            case TimeInterval.Kind.OPEN:
                return f'({self._start}; {self._end})'

            case TimeInterval.Kind.CLOSED:
                return f'[{self._start}; {self._end}]'
            
            case TimeInterval.Kind.CLOSED_OPEN:
                return f'[{self._start}; {self._end})'

            case TimeInterval.Kind.OPEN_CLOSED:
                return f'({self._start}; {self._end}]'
            
            case TimeInterval.Kind.RIGHT_OPEN:
                return f'({self._start}; +\u221E)'

            case TimeInterval.Kind.RIGHT_CLOSED:
                return f'[{self._start}; +\u221E)'

            case TimeInterval.Kind.LEFT_OPEN:
                return f'(-\u221E; {self._end})'

            case TimeInterval.Kind.LEFT_CLOSED:
                return f'(-\u221E; {self._end}]'
            
            case TimeInterval.Kind.TIMELINE:
                return '(-\u221E; +\u221E)'

        raise AssertionError(f'Unhandled \'TimeInterval.Kind\': {self._kind}.')

        
    
    def __bool__(self) -> bool:
        '''Checks whether the interval is non-empty.'''

        return self._kind is not TimeInterval.Kind.EMPTY
    

    def __len__(self) -> int:
        '''Returns the number of connected components.
        
        An empty interval has no connected components, while a non-empty
        interval has only one connected component.'''

        return int(self._kind is not TimeInterval.Kind.EMPTY)
    

    def __contains__(self, moment: object) -> bool:
        '''Checks whether the given moment in time falls within the time
        interval.'''

        if not isinstance(moment, Timestamp):
            return False

        if self.is_empty:
            return False
        
        if self._kind is TimeInterval.Kind.POINT:
            return moment == self._start

        if self._kind is TimeInterval.Kind.TIMELINE:
            return True
        # From this point onwards, the interval is considered to be
        # non-empty; it is neither a point nor the entire timeline.
        
        left_ok = True
        right_ok = True

        if self._start is not None:
            if self._kind in {
                TimeInterval.Kind.OPEN,
                TimeInterval.Kind.OPEN_CLOSED,
                TimeInterval.Kind.RIGHT_OPEN
            }:
                left_ok = moment > self._start
            else:
                left_ok = moment >= self._start

        if self._end is not None:
            if self._kind in {
                TimeInterval.Kind.OPEN,
                TimeInterval.Kind.CLOSED_OPEN,
                TimeInterval.Kind.LEFT_OPEN
            }:
                right_ok = moment < self._end
            else:
                right_ok = moment <= self._end
            
        return left_ok and right_ok
    

    def __and__(self, other: TimeInterval) -> TimeInterval:
        '''The intersection of two time intervals.'''

        # Quick checks for empty/timeline.
        if self.is_empty or other.is_empty:
            # An intersection with an empty interval is empty.
            return TimeInterval.empty()
        if self.is_timeline:
            return other
        if other.is_timeline:
            return self
        # From this point onwards, intervals are considered to be
        # non-empty and not to be the entire timeline.

        # Calculating the start of the intersection.
        if self.is_start_specified and other.is_start_specified:
            # The start of each interval is explicitly specified
            # (neither lies at infinity).

            # Calculating the start of the intersection.
            assert self.start is not None
            assert other.start is not None
            new_start = max(self.start, other.start)

            # Calculating whether or not the start is included
            # in the intersection.
            assert self.is_start_included is not None
            assert other.is_start_included is not None
            if self.start < other.start:
                new_is_start_included = other.is_start_included
            elif self.start > other.start:
                new_is_start_included = self.is_start_included
            else:
                new_is_start_included = self.is_start_included and other.is_start_included
        elif not self.is_start_specified and not other.is_start_specified:
            # The start of each interval is not specified (they lie
            # at infinity).
            
            new_start = None
            new_is_start_included = None
        else:
            # Only the start of one of the intervals is specified
            # (it doesn't lie at infinity).

            # Calculating the start of the intersection.
            new_start = self.start or other.start

            # Calculating whether or not the start is included
            # in the intersection.
            new_is_start_included = (
                self.is_start_included
                if self.is_start_specified
                else other.is_start_included
            )        

        # Calculating the end of the intersection.
        if self.is_end_specified and other.is_end_specified:
            # The end of each interval is explicitly specified
            # (neither lies at infinity).

            # Calculating the end of the intersection.
            assert self.end is not None
            assert other.end is not None
            new_end = min(self.end, other.end)

            # Calculating whether or not the end is included
            # in the intersection.
            assert self.is_end_included is not None
            assert other.is_end_included is not None
            if self.end < other.end:
                new_is_end_included = self.is_end_included
            elif self.end > other.end:
                new_is_end_included = other.is_end_included
            else:
                new_is_end_included = self.is_end_included and other.is_end_included
        elif not self.is_end_specified and not other.is_end_specified:
            # The end of each interval is not specified (they lie
            # at infinity).
            
            new_end = None
            new_is_end_included = None
        else:
            # Only the end of one of the intervals is specified
            # (it doesn't lie at infinity).

            # Calculating the end of the intersection.
            new_end = self.end or other.end

            # Calculating whether or not the end is included
            # in the intersection.
            new_is_end_included = (
                self.is_end_included
                if self.is_end_specified
                else other.is_end_included
            )  

        # Checking the resulting intersection for emptiness.
        if new_start is not None and new_end is not None:
            # The start and end of the intersection are clearly
            # specified, i.e. they do not lie at infinity.
            if new_is_start_included is True and new_is_end_included is True:
                # Both intersection boundaries are included.
                if new_start > new_end:
                    return TimeInterval.empty()
            else:
                # At least one intersection boundary is not included.
                if new_start >= new_end:
                    return TimeInterval.empty()
                
        return TimeInterval.from_boundaries(
            start=new_start,
            end=new_end,
            start_included=new_is_start_included,
            end_included=new_is_end_included
        )
    

    @classmethod
    def empty(cls) -> TimeInterval:
        '''Creates empty time interval.'''

        return cls(_kind=TimeInterval.Kind.EMPTY, _start=None, _end=None)


    @classmethod
    def point(cls, moment: Timestamp) -> TimeInterval:
        '''Creates a point. It corresponds to an instantaneous event.'''

        return cls(_kind=TimeInterval.Kind.POINT, _start=moment, _end=moment)
    

    @classmethod
    def open(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty open bounded time interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(_kind=TimeInterval.Kind.OPEN, _start=start, _end=end)
    

    @classmethod
    def closed(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty closed bounded time interval.
        Not a point.'''

        if start >= end:
            raise ValueError(
                'Either the start of the interval occurs after its end, which is incorrect, '
                'or it\'s a point.'
            )

        return cls(_kind=TimeInterval.Kind.CLOSED, _start=start, _end=end)
    

    @classmethod
    def closedopen(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty bounded closed-open interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(_kind=TimeInterval.Kind.CLOSED_OPEN, _start=start, _end=end)
    

    @classmethod
    def openclosed(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty bounded open-closed interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(_kind=TimeInterval.Kind.OPEN_CLOSED, _start=start, _end=end)
    

    @classmethod
    def rightclosed(cls, start: Timestamp) -> TimeInterval:
        '''Creates a closed right-ray.'''

        return cls(_kind=TimeInterval.Kind.RIGHT_CLOSED, _start=start)
    

    @classmethod
    def rightopen(cls, start: Timestamp) -> TimeInterval:
        '''Creates an open right-ray.'''

        return cls(_kind=TimeInterval.Kind.RIGHT_OPEN, _start=start)
    

    @classmethod
    def leftclosed(cls, end: Timestamp) -> TimeInterval:
        '''Creates a closed left ray.'''

        return cls(_kind=TimeInterval.Kind.LEFT_CLOSED, _end=end)
    

    @classmethod
    def leftopen(cls, end: Timestamp) -> TimeInterval:
        '''Creates an open left ray.'''

        return cls(_kind=TimeInterval.Kind.LEFT_OPEN, _end=end)
    

    @classmethod
    def timeline(cls) -> TimeInterval:
        '''Creates the entire timeline.'''

        return cls(_kind=TimeInterval.Kind.TIMELINE)
    

    @classmethod
    def from_boundaries(
        cls,
        start: Timestamp | None, end: Timestamp | None,
        start_included: bool | None, end_included: bool | None
    ) -> TimeInterval:
        '''Creates a time interval by its boundaries.
        
        If both boundaries are not specified, the interval is considered
        to be empty. It is not possible to set the entire timeline
        in this way. Use the 'timeline' constructor for this purpose.'''
        
        if start is not None and end is not None:
            # Both boundaries of the interval are specified. This is
            # a (non-empty) bounded interval.

            if start_included is None or end_included is None:
                raise ValueError(
                    'The interval is set incorrectly. If the boundaries are specified, they'
                    'must be included or excluded. The values \'start_included\' '
                    'and \'end_included\' must be \'True\' or \'False\'.'
                )

            if start_included and end_included:
                # Both the start and end of the interval are included.
                # This is a closed (non-empty, bounded) interval. This
                # may be the point.

                if start > end:
                    raise ValueError(
                        'The interval is set incorrectly. The start of the interval cannot occur '
                        'later than the end.'
                    )
                
                if start == end:
                    # The interval is a point.
                    return TimeInterval.point(start)
                
                return TimeInterval.closed(start, end)
            
            elif start_included and not end_included:
                # The start of the interval is included, but the end
                # is not. This is a closed-open (non-empty, bounded)
                # interval.

                if start >= end:
                    raise ValueError(
                        'The interval is set incorrectly. The start of the closed-open interval '
                        'cannot occur after the end, or coincide with it.'
                    )

                return TimeInterval.closedopen(start, end)
            
            elif not start_included and end_included:
                # The start of the interval is not included, but the end
                # is included. This is an open-closed (non-empty,
                # bounded) interval.

                if start >= end:
                    raise ValueError(
                        'The interval is set incorrectly. The start of the open-closed interval '
                        'cannot occur after the end, or coincide with it.'
                    )

                return TimeInterval.openclosed(start, end)
            
            else:
                # Both the start and end of the interval
                # are not included. This is an open (non-empty, bounded)
                # interval.

                if start >= end:
                    raise ValueError(
                        'The interval is set incorrectly. The start of the open interval cannot '
                        'occur after the end, or coincide with it.'
                    )

                return TimeInterval.open(start, end)
                
        elif start is not None and end is None:
            # The start of the interval is specified, but not the end.
            # This is the right-ray.

            if end_included is not None:
                raise ValueError(
                    'The interval is set incorrectly. If the end is not specified, it cannot be '
                    'included or excluded. The value \'end_included\' must be \'None\'.'
                )
            
            if start_included:
                # The start of the interval is included. This is a right
                # closed ray.

                return TimeInterval.rightclosed(start)
            else:
                # The start of the interval is not included. This is
                # a right open ray.
                
                return TimeInterval.rightopen(start)
            
        elif start is None and end is not None:
            # The start of the interval is not specified, but
            # its end is. This is the left ray.

            if start_included is not None:
                raise ValueError(
                    'The interval is set incorrectly. If the start is not specified, it cannot be '
                    'included or excluded. The value \'start_included\' must be \'None\'.'
                )
            
            if end_included:
                # The end of the interval is included. This is a left
                # closed ray.

                return TimeInterval.leftclosed(end)
            else:
                # The end of the interval is not included. This is
                # a left open ray.
                
                return TimeInterval.leftopen(end)
            
        else:
            # The start and end of the interval are not specified.

            if start_included is not None or end_included is not None:
                raise ValueError(
                    'The interval is set incorrectly. If the boundaries are not specified, they '
                    'cannot be included or excluded. The values \'start_included\' '
                    'and \'end_included\' must be \'None\'.'
                )
            
            # If both boundaries are not specified, the interval
            # is considered to be empty.
            return TimeInterval.empty()
    

    @classmethod
    def minimal_cover(cls, *intervals: TimeInterval) -> TimeInterval:
        '''Creates the smallest interval containing the given ones.
        
        This is not a cover in the strict topological sense because
        it creates a single interval rather than a union.'''

        # Remove empty intervals.
        nonempty_intervals = [i for i in intervals if not i.is_empty]

        # If there are no non-empty intervals, then the cover is empty.
        if not nonempty_intervals:
            return cls.empty()

        # Find the left boundary, i.e. minimal start. ('None'
        # is considered the smallest because it denotes a boundary that
        # lies at infinity.)
        def start_key(i: TimeInterval):
            return (i.start is not None, i.start)

        leftmost = min(nonempty_intervals, key=start_key)

        start = leftmost.start

        # The start is included if it is included in at least one
        # of the intervals.
        start_included = (
            None if start is None
            else any(
                i.start == start and i.is_start_included
                for i in nonempty_intervals
            )
        )

        # Find the right boundary, i.e. maximal end. ('None'
        # is considered the largest because it denotes a boundary that
        # lies at infinity.)
        def end_key(i: TimeInterval):
            return (i.end is None, i.end)

        rightmost = max(nonempty_intervals, key=end_key)

        end = rightmost.end

        # The end is included if it is included in at least one
        # of the intervals.
        end_included = (
            None if end is None
            else any(
                i.end == end and i.is_end_included
                for i in nonempty_intervals
            )
        )

        # Construct the covering interval.
        return cls.from_boundaries(
            start=start,
            end=end,
            start_included=start_included,
            end_included=end_included
        )



    @property
    def is_nonempty(self) -> bool:
        '''Checks whether the time interval is non-empty.'''

        return not self._kind is TimeInterval.Kind.EMPTY


    @property
    def is_empty(self) -> bool:
        '''Checks whether the time interval is empty.'''

        return self._kind is TimeInterval.Kind.EMPTY
    

    @property
    def is_bounded(self) -> bool:
        '''Checks whether the interval is bounded.
        
        Here, boundedness is understood in a mathematical sense.
        Therefore an empty interval is considered to be bounded.'''

        return self._kind in TimeInterval._BOUNDED_KINDS
    

    @property
    def is_left_bounded(self) -> bool:
        '''Checks whether the interval is bounded on the left.'''

        return self._kind in TimeInterval._LEFT_BOUNDED_KINDS
    

    @property
    def is_right_bounded(self) -> bool:
        '''Checks whether the interval is bounded on the right.'''

        return self._kind in TimeInterval._RIGHT_BOUNDED_KINDS


    @property
    def is_connected(self) -> bool:
        '''Any interval is connected.'''

        return True
    

    @property
    def is_point(self) -> bool:
        '''Checks whether the time interval is a point.'''

        return self._kind is TimeInterval.Kind.POINT
    

    @property
    def is_timeline(self) -> bool:
        '''Checks whether the time interval is the entire timeline.'''

        return self._kind is TimeInterval.Kind.TIMELINE
    

    @property
    def is_open(self) -> bool:
        '''Checks whether the time interval is open.
        
        Here, openness is understood in a mathematical sense. Therefore,
        an empty interval, a bounded open interval, an open ray
        and the entire timeline are all considered to be open sets.'''

        return self._kind in TimeInterval._OPEN_KINDS
    

    @property
    def is_closed(self) -> bool:
        '''Checks whether the time interval is closed.

        Here, closeness is understood in a mathematical sense.
        Therefore, an empty interval, a bounded closed interval,
        a closed ray and the entire timeline are all considered
        to be closed sets.'''

        return self._kind in TimeInterval._CLOSED_KINDS
    

    @property
    def start(self) -> Timestamp | None:
        '''Returns the start of the time interval. If it is not defined,
        when the interval is empty or unbounded
        on the left, returns 'None'.'''

        return self._start
    

    @property
    def is_start_specified(self) -> bool:
        '''Returns whether the start of the interval is specified.'''

        return self._kind in TimeInterval._START_SPECIFIED_KINDS


    @property
    def is_start_included(self) -> bool | None:
        '''If the start of the interval is specified, returns whether
        it is included in the interval or not. If the start
        is not specified, it returns 'None'.'''

        if self._kind in TimeInterval._START_SPECIFIED_KINDS:
            return self._kind in TimeInterval._START_INCLUDED_KINDS
        else:
            return None
    

    @property
    def end(self) -> Timestamp | None:
        '''Returns the end of the time interval. If it is not defined,
        when the interval is empty or unbounded
        on the right, returns 'None'.'''

        return self._end
    

    @property
    def is_end_specified(self) -> bool:
        '''Returns whether the end of the interval is specified.'''

        return self._kind in TimeInterval._END_SPECIFIED_KINDS
    

    @property
    def is_end_included(self) -> bool | None:
        '''If the end of the interval is specified, returns whether
        it is included in the interval or not. If the end
        is not specified, it returns 'None'.'''

        if self._kind in TimeInterval._END_SPECIFIED_KINDS:
            return self._kind in TimeInterval._END_INCLUDED_KINDS
        else:
            return None


    @property
    def inf(self) -> Timestamp | None:
        '''Returns the infimum of the time interval. If it is
        not defined, when the interval is empty or unbounded
        on the left, returns 'None'.'''

        return self._start
    

    @property
    def sup(self) -> Timestamp | None:
        '''Returns the supremum of the time interval. If it is
        not defined, when the interval is empty or unbounded
        on the right, returns 'None'.'''
        
        return self._end
    

    @property
    def duration(self) -> datetime.timedelta | None:
        '''Determines the duration of the time interval.

        If the duration is not defined (in the case of an unbounded
        interval), returns 'None'. The duration of an empty interval
        is zero.'''

        if self.is_bounded:
            # The interval is bounded.
            if self.is_empty:
                # The interval is empty.
                return datetime.timedelta()
            else:
                # The interval is bounded and non-empty.
                if self._start is None or self._end is None:
                    # At least one of the interval boundaries has not
                    # been specified, which contradicts its boundedness.
                    raise ValueError(
                        'It is impossible to determine the start or end of the time interval.'
                    )

                return self._end - self._start
        else:
            return None
        
    
    def contains(self, other: TimeInterval) -> bool:
        '''Checks whether this interval contains another one.'''

        # Any contains an empty interval.
        if other.is_empty:
            return True
        # From this point onwards, we will consider 'other' to be
        # non-empty. An empty interval cannot contain a non-empty one.

        if self.is_empty:
            return False

        # Checking the left boundary.
        if other.start is not None:
            # 'other' is bounded on the left.
            if self.start is None:
                # 'self' is unbounded on the left.
                left_ok = True
            else:
                # 'self' is bounded on the left.

                if self.start < other.start:
                    left_ok = True
                elif self.start == other.start:
                    left_ok = (
                        self.is_start_included is True
                        or other.is_start_included is False
                    )
                else:
                    left_ok = False
        else:
            # 'other' is unbounded on the left.
            left_ok = self.start is None

        if not left_ok:
            return False

        # Checking the right boundary.
        if other.end is not None:
            # 'other' is bounded on the right.
            if self.end is None:
                # 'self' is unbounded on the right.
                right_ok = True
            else:
                # 'self' is bounded on the right.

                if self.end > other.end:
                    right_ok = True
                elif self.end == other.end:
                    right_ok = (
                        self.is_end_included is True
                        or other.is_end_included is False
                    )
                else:
                    right_ok = False
        else:
            # 'other' is unbounded on the right.
            right_ok = self.end is None

        return right_ok
    

    def is_contained_in(self, other: TimeInterval) -> bool:
        '''Checks whether a given interval is contained in another.'''

        return other.contains(self)
    

    def is_left_of(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the left
        of the other one and does not intersect with it.
        
        This is automatically true if any of the time intervals
        are empty.'''

        if self.is_empty or other.is_empty:
            return True
        # From this point onwards, both intervals are considered
        # to be non-empty.

        if self.end is not None and other.start is not None:
            # 'self' is bounded on the right and 'other' is bounded
            # on the left.

            if self.end < other.start:
                return True
            if self.end == other.start:
                return self.is_end_included is False or other.is_start_included is False
            
        return False
    

    def is_right_of(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the right
        of the other one and does not intersect with it.
        
        This is automatically true if any of the time intervals
        are empty.'''

        if self.is_empty or other.is_empty:
            return True
        # From this point onwards, both intervals are considered
        # to be non-empty.

        if other.end is not None and self.start is not None:
            # 'other' is bounded on the right and 'self' is bounded
            # on the left.

            if other.end < self.start:
                return True
            if other.end == self.start:
                return other.is_end_included is False or self.is_start_included is False
            
        return False
    

    def is_left_of_disconnectedly(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the left
        of the other one, does not intersect with it and their union
        will be a disconnected set.
        
        This is not true if at least one of the intervals is empty,
        because a disconnected union is required.'''

        if self.is_empty or other.is_empty:
            return False
        # From this point onwards, both intervals are considered
        # to be non-empty.
        
        if self.end is not None and other.start is not None:
            # 'self' is bounded on the right and 'other' is bounded
            # on the left.

            if self.end < other.start:
                return True
            if self.end == other.start:
                return self.is_end_included is False and other.is_start_included is False
            
        return False
    

    def is_right_of_disconnectedly(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the right
        of the other one, does not intersect with it and their union
        will be a disconnected set.
        
        This is not true if at least one of the intervals is empty,
        because a disconnected union is required.'''

        if self.is_empty or other.is_empty:
            return False
        # From this point onwards, both intervals are considered
        # to be non-empty.
        
        if other.end is not None and self.start is not None:
            # 'other' is bounded on the right and 'self' is bounded
            # on the left.

            if other.end < self.start:
                return True
            if other.end == self.start:
                return other.is_end_included is False and self.is_start_included is False
            
        return False
    

    def overlaps(self, other: TimeInterval) -> bool:
        '''Check if two intervals overlap (have non-empty
        intersection).'''
        
        if self.is_empty or other.is_empty:
            return False
        # From this point onwards, both intervals are considered
        # to be non-empty.

        # Check whether 'self' is completely to the left of 'other'.
        if self.is_left_of(other):
            return False

        # Check whether 'other' is completely to the left of 'self'.
        if other.is_left_of(self):
            return False

        return True
    

    def touches(self, other: TimeInterval) -> bool:
        '''Checks whether the intervals touch each other.
        
        In other words, it checks whether they are non-empty and whether
        their union is connected.'''

        if self.is_empty or other.is_empty:
            return False
        # From this point onwards, both intervals are considered
        # to be non-empty.

        # Check whether 'self' is completely to the left of 'other'
        # and their union is disconnected.
        if self.is_left_of_disconnectedly(other):
            return False

        # Check whether 'other' is completely to the left of 'self'
        # and their union is disconnected.
        if other.is_left_of_disconnectedly(self):
            return False

        return True



@dataclass(frozen=True)
class TimeSet:
    '''Disjoint union of time intervals 'TimeInterval'.
    
    All component intervals must be non-empty and in chronological 
    order. They must also not overlap. Furthermore, all their pairwise
    unions must be disconnected. In other words, each interval
    is connected component of the 'TimeSet'.'''

    _intervals: tuple[TimeInterval]


    def _is_valid(self) -> bool:
        '''Checks whether the 'TimeSet' is set correctly.'''

        # 'TimeSet' must not contain any empty intervals.
        for i in self._intervals:
            if i.is_empty:
                return False

        # The intervals in 'TimeSet' must be chronologically ordered,
        # and all their pairwise unions must be disconnected.
        for l, r in zip(self._intervals, self._intervals[1:]):
            if not l.is_left_of_disconnectedly(r):
                return False
        
        return True
    

    def __init__(self, *intervals: TimeInterval):
        object.__setattr__(self, '_intervals', tuple(intervals))

        if not self._is_valid():
            raise ValueError('The \'TimeSet\' has been set incorrectly.')
    

    def __str__(self) -> str:
        if self.is_empty:
            # Returns the empty set symbol.
            return '\u2205'
        else:
            return ' \u2294\n'.join(map(str, self._intervals))
    

    def __bool__(self) -> bool:
        '''Checks whether the time set is non-empty.'''

        return bool(self._intervals)
    

    def __len__(self) -> int:
        '''Returns the number of connected components.'''

        return len(self._intervals)
    

    @classmethod
    def union(cls, *intervals: TimeInterval) -> TimeSet:
        '''Creates a 'TimeSet' as a union of 'TimeInterval' time
        intervals.'''

        # Remove empty intervals.
        nonempty_intervals = [i for i in intervals if not i.is_empty]

        # If there are no non-empty intervals, then the union is empty.
        if not nonempty_intervals:
            return cls()

        # Sort intervals chronologically (by their starts).
        def start_key(i: TimeInterval):
            return (i.start is not None, i.start)

        nonempty_intervals.sort(key=start_key)

        # Group touching intervals.
        components: list[list[TimeInterval]] = []
        current_group = [nonempty_intervals[0]]

        for interval in nonempty_intervals[1:]:
            if current_group[-1].touches(interval):
                # If the new interval touches the last interval
                # in the group, add it to the group.
                current_group.append(interval)
            else:
                # If the new interval does not touch the last interval
                # in the group, we keep the previous group and create
                # a new one that includes the new interval.
                components.append(current_group)
                current_group = [interval]

        # Keep the last group.
        components.append(current_group)

        # Build minimal covers (connected components).
        merged_intervals = [
            TimeInterval.minimal_cover(*group)
            for group in components
        ]

        # Construct 'TimeSet'.
        return cls(*merged_intervals)
    

    @property
    def is_nonempty(self) -> bool:
        '''Checks whether the time set is non-empty.'''

        return bool(self._intervals)


    @property
    def is_empty(self) -> bool:
        '''Checks whether the time set is empty.'''

        return not self._intervals
    

    @property
    def is_bounded(self) -> bool:
        '''Checks whether the time set is bounded.
        
        Here, boundedness is understood in a mathematical sense.
        Therefore an empty set is considered bounded.'''

        if self.is_empty:
            return True
        # From this point onwards, the set is considered
        # to be non-empty.

        # Check the first and last intervals for boundedness.
        return self._intervals[0].is_bounded and self._intervals[-1].is_bounded


    @property
    def is_connected(self) -> bool:
        '''Checks whether the time set is connected.
        
        A time set is connected if it has no more than one connected 
        component.'''

        return len(self) <= 1
    

    @property
    def is_point(self) -> bool:
        '''Checks whether the time set is a point.'''

        return len(self) == 1 and self._intervals[0].is_point
    

    @property
    def is_open(self) -> bool:
        '''Checks whether the time set is open.'''

        if self.is_empty:
            return True
        # From this point onwards, the set is considered
        # to be non-empty.

        return all(i.is_open for i in self._intervals)
    

    @property
    def is_closed(self) -> bool:
        '''Checks whether the time set is closed.'''

        if self.is_empty:
            return True
        # From this point onwards, the set is considered to be
        # non-empty.

        return all(i.is_closed for i in self._intervals)