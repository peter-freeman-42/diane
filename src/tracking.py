from __future__ import annotations
from dataclasses import dataclass, field
from functools import total_ordering
from typing import overload
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
        
        return (
            self._dt.astimezone(zoneinfo.ZoneInfo("Etc/UTC")) ==
            other._dt.astimezone(zoneinfo.ZoneInfo("Etc/UTC"))
        )
    

    def __lt__(self, other: object) -> bool:
        '''Less-than comparison based on absolute (UTC) time.'''
    
        if not isinstance(other, Timestamp):
            return NotImplemented
        
        return (
            self._dt.astimezone(zoneinfo.ZoneInfo("Etc/UTC")) <
            other._dt.astimezone(zoneinfo.ZoneInfo("Etc/UTC"))
        )
    

    def __add__(self, other: datetime.timedelta) -> Timestamp:
        '''Time shift by a specified interval.'''

        if not isinstance(other, datetime.timedelta):
            return NotImplemented

        tz = self._dt.tzinfo
        dt_utc = self._dt.astimezone(zoneinfo.ZoneInfo('Etc/UTC'))
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
            self_dt_utc = self._dt.astimezone(zoneinfo.ZoneInfo('Etc/UTC'))
            other_dt_utc = other._dt.astimezone(zoneinfo.ZoneInfo('Etc/UTC'))
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

        tz_utc = zoneinfo.ZoneInfo("Etc/UTC")

        if dt.tzinfo is None:
            # Naive: interpret as UTC per method contract.
            dt = dt.replace(tzinfo=tz_utc)
        else:
            # Ensure the moment is UTC (offset zero).
            if dt.utcoffset() != datetime.timedelta(0):
                raise ValueError(f"The timestamp '{dt_iso}' is not in UTC.")
            # Convert to 'ZoneInfo('Etc/UTC')' to satisfy strict storage
            # invariant.
            dt = dt.astimezone(tz_utc)

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
            return cls(datetime.datetime.now(zoneinfo.ZoneInfo('Etc/UTC')))
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
        '''Returns the local time zone in IANA format.'''

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
        
        dt_utc = self._dt.astimezone(zoneinfo.ZoneInfo('Etc/UTC'))
        return Timestamp(dt_utc)
    

    @property
    def utc_iso(self) -> str:
        '''Returns an ISO 8601 string in UTC.'''
    
        dt_utc = self._dt.astimezone(zoneinfo.ZoneInfo("Etc/UTC"))
        return dt_utc.replace(tzinfo=None).isoformat() + 'Z'



@dataclass(frozen=True)
class TimeInterval:
    '''A time interval.

    This can be an empty interval, a point, an open interval,
    a half-open interval, a segment, an open or closed ray,
    or the entire timeline. Here, openness, closedness and boundedness
    are understood in a strict mathematical sense.
    
    If the interval is empty, then '_nonempty' is set to 'False' and all
    the other fields are set to 'None'. If the interval is not empty,
    then '_nonempty' is True and all the other fields must define
    a valid interval.'''


    _nonempty: bool = False
    _start: Timestamp | None = None
    _end: Timestamp | None = None
    _start_closed: bool | None = None
    _end_closed: bool | None = None


    def _is_valid(self) -> bool:
        '''Checks whether the time interval is set correctly.'''
        
        if self._nonempty:
            # The interval is non-empty.

            # Verifying the correctness of boundary type assignments.
            if self._start is None:
                # The start of the interval is not specified, meaning
                # it is at infinity.
                if self._start_closed is not False:
                    # In this case, it is assumed that the boundary
                    # is open.
                    return False
            else:
                # The start of the interval is explicitly specified
                # (it does not lie at infinity).
                if self._start_closed is None:
                    # In this case, the boundary type must be specified
                    # as either open or closed. Therefore,
                    # '_start_closed' cannot be 'None'.
                    return False
            
            if self._end is None:
                # The end of the interval is not specified, meaning
                # it is at infinity.
                if self._end_closed is not False:
                    # In this case, it is assumed that the boundary
                    # is open.
                    return False
            else:
                # The end of the interval is explicitly specified
                # (it does not lie at infinity).
                if self._end_closed is None:
                    # In this case, the boundary type must be specified
                    # as either open or closed. Therefore,
                    # '_end_closed' cannot be 'None'.
                    return False

            # Validation of the correctness of the time interval
            # specification when both boundaries are specified.
            if self._start is not None and self._end is not None:
                # The boundaries of the interval are both specified
                # (not at infinity).
                if self._start == self._end:
                    # The start of the interval coincides with its end.
                    if self._start_closed is False or self._end_closed is False:
                        # At least one boundary is open. In this case,
                        # however, the interval is empty, which 
                        # is inconsistent with the value '_nonempty'.
                        return False
                    
                if self._start > self._end:
                    # The start of the interval is later than its end.
                    # In this case, however, the interval is empty,
                    # which is inconsistent with the value '_nonempty'.
                    return False
            
            # Validation of the correctness of the time interval
            # specification when both boundaries of the interval
            # are not specified (they are both at infinity).
            if self._start is None and self._end is None:
                # The boundaries of the interval are both not specified
                # (are at infinity).
                if self._start_closed is not False or self._end_closed is not False:
                    # At least one boundary is not open. If the interval
                    # is non-empty and its boundaries are not specified
                    # (i.e. if they are both at infinity), then
                    # the interval is supposed to be the entire
                    # timeline. In this case, it is considered that both
                    # borders are open.
                    return False
        else:
            # If the interval is empty, all fields except '_nonempty'
            # must be set to 'None'.
            if any(x is not None for x in (
                self._start, self._end, self._start_closed, self._end_closed
            )):
                return False
        return True
    

    def __post_init__(self) -> None:
        if not self._is_valid():
            raise ValueError('The time interval has been set incorrectly.')
        
    
    def __str__(self) -> str:
        if self.is_empty:
            # Returns the empty set symbol.
            return '\u2205'
        else:
            if self.is_point:
                return f'{{{self._start}}}'
            
            start_bracket = '[' if self._start_closed is True else '('
            end_bracket = ']' if self._end_closed is True else ')'
            start_str = '-\u221E' if self._start is None else str(self._start)
            end_str = '+\u221E' if self._end is None else str(self._end)
        
            return f'{start_bracket}{start_str}; {end_str}{end_bracket}'
        
    
    def __bool__(self) -> bool:
        '''Checks whether the interval is non-empty.'''

        return not self.is_empty
    

    def __contains__(self, moment: Timestamp) -> bool:
        '''Checks whether the given moment in time falls within the time
        interval.'''

        if self.is_empty:
            # An empty time interval contains nothing.
            return False
        
        if self._start is not None:
            if self._start_closed:
                if moment < self._start:
                    return False
            else:
                if moment <= self._start:
                    return False

        if self._end is not None:
            if self._end_closed:
                if moment > self._end:
                    return False
            else:
                if moment >= self._end:
                    return False

        return True
    

    def __and__(self, other: TimeInterval) -> TimeInterval:
        '''The intersection of two time intervals.'''

        if self.is_empty or other.is_empty:
            # An intersection with an empty interval is empty.
            return TimeInterval.empty()
        
        # Calculating the start of the intersection. From this point
        # onwards, intervals are considered to be non-empty.
        if self._start is not None and other._start is not None:
            # The start of each interval is explicitly defined (neither
            # lies at infinity).

            # Calculating the start of the intersection.
            new_start = max(self._start, other._start)

            # Calculating the type of start of intersection of intervals
            # (open or closed).
            if self._start < other._start:
                new_start_closed = other._start_closed
            elif self._start > other._start:
                new_start_closed = self._start_closed
            else:
                new_start_closed = self._start_closed and other._start_closed
        else:
            # The start of one of the intervals is not explicitly
            # specified (it lies at infinity).

            # Calculating the start of the intersection.
            new_start = self._start or other._start

            # Calculating the type of start of intersection of intervals
            # (open or closed).
            new_start_closed = self._start_closed and other._start_closed
        

        # Calculating the end of the intersection.
        if self._end is not None and other._end is not None:
            # The end of each interval is explicitly defined (neither
            # lies at infinity).

            # Calculating the end of the intersection.
            new_end = min(self._end, other._end)

            # Calculating the type of end of intersection of intervals
            # (open or closed).
            if self._end < other._end:
                new_end_closed = self._end_closed
            elif self._end > other._end:
                new_end_closed = other._end_closed
            else:
                new_end_closed = self._end_closed and other._end_closed
        else:
            # The end of one of the intervals is not explicitly
            # specified (it lies at infinity).

            # Calculating the end of the intersection.
            new_end = self._end or other._end

            # Calculating the type of end of intersection of intervals
            # (open or closed).
            new_end_closed = self._end_closed and other._end_closed

        # Checking that the resulting intersection is empty.
        if new_start is not None and new_end is not None:
            # The assumed start and end of the intersection
            # are not at infinity.
            if new_start_closed and new_end_closed:
                if new_start > new_end:
                    return TimeInterval.empty()
            else:
                if new_start >= new_end:
                    return TimeInterval.empty()
                
        return TimeInterval(True, new_start, new_end, new_start_closed, new_end_closed)
    

    @classmethod
    def empty(cls) -> TimeInterval:
        '''Creates empty time interval.'''

        return cls(_nonempty=False, _start=None, _end=None, _start_closed=None, _end_closed=None)


    @classmethod
    def point(cls, moment: Timestamp) -> TimeInterval:
        '''Creates a point. It corresponds to an instantaneous event.'''

        return cls(
            _nonempty=True, _start=moment, _end=moment, _start_closed=True, _end_closed=True
        )
    

    @classmethod
    def open(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates an non-empty open bounded time interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(
            _nonempty=True, _start=start, _end=end, _start_closed=False, _end_closed=False
        )
    

    @classmethod
    def closed(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates an non-empty closed bounded time interval.'''

        if start > end:
            raise ValueError(
                'The start of the interval occurs after its end, which is incorrect.'
            )

        return cls(
            _nonempty=True, _start=start, _end=end, _start_closed=True, _end_closed=True
        )
    

    @classmethod
    def closedopen(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty bounded closed-open interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(
            _nonempty=True, _start=start, _end=end, _start_closed=True, _end_closed=False
        )
    

    @classmethod
    def openclosed(cls, start: Timestamp, end: Timestamp) -> TimeInterval:
        '''Creates a non-empty bounded open-closed interval.'''

        if start >= end:
            raise ValueError(
                'The start of the interval is either simultaneous with its end or occurs later, '
                'which is not correct.'
            )

        return cls(
            _nonempty=True, _start=start, _end=end, _start_closed=False, _end_closed=True
        )
    

    @classmethod
    def rightclosed(cls, start: Timestamp) -> TimeInterval:
        '''Creates a closed right ray.'''

        return cls(
            _nonempty=True, _start=start, _end=None, _start_closed = True, _end_closed=False
        )
    

    @classmethod
    def rightopen(cls, start: Timestamp) -> TimeInterval:
        '''Creates an open right ray.'''

        return cls(
            _nonempty=True, _start=start, _end=None, _start_closed = False, _end_closed=False
        )
    
    @classmethod
    def leftclosed(cls, end: Timestamp) -> TimeInterval:
        '''Creates a closed left ray.'''

        return cls(
            _nonempty=True, _start=None, _end=end, _start_closed = False, _end_closed=True
        )
    

    @classmethod
    def leftopen(cls, end: Timestamp) -> TimeInterval:
        '''Creates an open left ray.'''

        return cls(
            _nonempty=True, _start=None, _end=end, _start_closed = False, _end_closed=False
        )
    

    @classmethod
    def timeline(cls) -> TimeInterval:
        '''Creates the entire timeline.'''

        return cls(
            _nonempty=True, _start=None, _end=None, _start_closed = False, _end_closed=False
        )


    @property
    def is_empty(self) -> bool:
        '''Checks whether the time interval is empty.'''

        return not self._nonempty
    

    @property
    def is_bounded(self) -> bool:
        '''Checks whether the interval is bounded.
        
        Here, boundedness is understood in a mathematical sense.
        Therefore an empty interval is considered bounded.'''

        return not self._nonempty or (
            self._start is not None
            and self._end is not None
        )
    

    @property
    def is_point(self) -> bool:
        '''Checks whether the time interval is a point.'''

        return (
            self._nonempty
            and self._start is not None
            and self._end is not None
            and self._start == self._end
            and self._start_closed is True
            and self._end_closed is True
        )
    

    @property
    def is_open(self) -> bool:
        '''Checks whether the time interval is open.
        
        Here, openness is understood in a mathematical sense. Therefore,
        an empty interval, a bounded open interval, an open ray
        and the entire timeline are all considered to be open sets.'''

        return not self._nonempty or (
            self._start_closed is False
            and self._end_closed is False
        )
    

    @property
    def is_closed(self) -> bool:
        '''Checks whether the time interval is closes.

        Here, closeness is understood in a mathematical sense.
        Therefore, an empty interval, a bounded closed interval,
        a closed ray and the entire timeline are all considered
        to be closed sets.'''

        return not self._nonempty or (
            # A non-empty closed interval (including a point).
            self._start is not None
            and self._end is not None
            and self._start_closed is True
            and self._end_closed is True
        ) or (
            # The entire timeline.
            self._start is None
            and self._end is None
            and self._start_closed is False
            and self._end_closed is False
        ) or (
            # A right closed ray.
            self._start is not None
            and self._end is None
            and self._start_closed is True
            and self._end_closed is False
        ) or (
            # A left closed ray.
            self._start is None
            and self._end is not None
            and self._start_closed is False
            and self._end_closed is True
        )
    

    @property
    def start(self) -> Timestamp | None:
        '''Returns the start of the time interval. If it is not defined,
        when the interval is empty or unbounded
        on the left, returns 'None'.'''

        return self._start
    

    @property
    def end(self) -> Timestamp | None:
        '''Returns the end of the time interval. If it is not defined,
        when the interval is empty or unbounded
        on the right, returns 'None'.'''

        return self._end


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

        # From this point onwards, we will consider 'other'
        # to be non-empty. An empty interval cannot contain a non-empty
        # one.
        if self.is_empty:
            return False

        # Checking the left boundary.
        if other._start is not None:
            if self._start is None:
                left_ok = True
            else:
                if self._start < other._start:
                    left_ok = True
                elif self._start == other._start:
                    left_ok = (
                        self._start_closed is True
                        or other._start_closed is False
                    )
                else:
                    left_ok = False
        else:
            # The start of 'other' lies in infinity.
            left_ok = self._start is None

        if not left_ok:
            return False

        # Checking the right boundary.
        if other._end is not None:
            if self._end is None:
                right_ok = True
            else:
                if self._end > other._end:
                    right_ok = True
                elif self._end == other._end:
                    right_ok = (
                        self._end_closed is True
                        or other._end_closed is False
                    )
                else:
                    right_ok = False
        else:
            # The end of 'other' lies in infinity.
            right_ok = self._end is None

        return right_ok
    

    def is_contained_in(self, other: TimeInterval) -> bool:
        '''Checks whether a given interval is contained in another.'''

        return other.contains(self)
    

    def is_left_of(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the left
        of the other one and does not intersect with it.'''

        if self.is_empty or other.is_empty:
            return True
        
        if self._end is not None and other._start is not None:
            if self._end < other._start:
                return True
            if self._end == other._start:
                return self._end_closed is False or other._start_closed is False
            
        return False
    

    def is_right_of(self, other: TimeInterval) -> bool:
        '''Checks that the interval lies strictly to the right
        of the other one and does not intersect with it.'''

        if self.is_empty or other.is_empty:
            return True
        
        if other._end is not None and self._start is not None:
            if other._end < self._start:
                return True
            if other._end == self._start:
                return other._end_closed is False or  self._start_closed is False
            
        return False
    

    def overlaps(self, other: TimeInterval) -> bool:
        '''Check if two intervals overlap (have non-empty
        intersection).'''
        
        if self.is_empty or other.is_empty:
            return False

        # 'self' is completely to the left of 'other'.
        if self._end is not None and other._start is not None:
            if self._end < other._start:
                return False
            if self._end == other._start:
                return self._end_closed is True and other._start_closed is True

        # 'other' is completely to the left of 'self'.
        if other._end is not None and self._start is not None:
            if other._end < self._start:
                return False
            if other._end == self._start:
                return other._end_closed is True and self._start_closed is True

        return True