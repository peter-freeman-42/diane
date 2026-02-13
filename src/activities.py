from __future__ import annotations
from dataclasses import dataclass, field



@dataclass
class Activity:
    '''Activity.'''
    
    _slug: str
    title: str
    description: str = field(default_factory=str)


    def __hash__(self):
        return hash(self._slug)
    

    def __eq__(self, other):

        if not isinstance(other, Activity):
            return NotImplemented
        
        return self.slug == other.slug
    

    @property
    def slug(self):
        '''Returns a unique string identifier of the activity.'''

        return self._slug