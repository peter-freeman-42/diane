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
        return f'{self.local_iso}, {self.timezone_iana}'
    

    def __lt__(self, other: Timestamp) -> bool:
        '''Less-than comparison based on absolute (UTC) time.'''
    
        if not isinstance(other, Timestamp):
            return NotImplemented
        
        return self._dt < other._dt
    

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
    def local(self) -> datetime.datetime:
        return self._dt
    

    @property
    def local_iso(self) -> str:
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