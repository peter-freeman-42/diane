from __future__ import annotations
from dataclasses import dataclass, field
import networkx as nx
import yaml
from pathlib import Path



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
    

    @classmethod
    def from_dict(cls, slug: str, data: dict) -> Activity:
        return cls(
            _slug=slug,
            title=data['title'],
            description=data.get('description', '')
        )
    


@dataclass
class Activities:
    '''All activities.'''

    activities_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    slug_to_activity: dict[str, Activity] = field(default_factory=dict[str, Activity])

    
    def validate(self) -> None:

        if not nx.is_directed_acyclic_graph(self.activities_graph):
            cycle = nx.find_cycle(self.activities_graph)
            raise ValueError(f'Cycle detected: {cycle}.')
    

    def clear(self) -> None:
        '''Clears all activities data.'''

        self.activities_graph.clear()
        self.slug_to_activity.clear()
        

    def load_from_yaml(self, filename: str) -> None:
        '''Load activities from YAML file.'''

        path = Path(filename)

        with path.open('r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            raise ValueError('YAML root must be a mapping.')

        activities_data = data.get('activities')
        if not isinstance(activities_data, dict):
            raise ValueError('\'activities\' must be a mapping.')

        # Clear old data.
        self.clear()

        # Create activities.
        for slug, item in activities_data.items():
            if not isinstance(item, dict):
                raise ValueError('Each activity must be a mapping.')

            activity = Activity.from_dict(slug, item)

            if activity.slug in self.slug_to_activity:
                raise ValueError(f'Duplicate slug: {activity.slug}.')

            self.slug_to_activity[activity.slug] = activity
            self.activities_graph.add_node(activity)

        # Create connections.
        for slug, item in activities_data.items():
            parents = item.get('parents', [])

            if parents is None:
                parents = []

            if not isinstance(parents, list):
                raise ValueError(
                    f'\'parents\' of \'{slug}\' must be a list.'
                )

            child = self.slug_to_activity[slug]

            for parent_slug in parents:
                if parent_slug not in self.slug_to_activity:
                    raise ValueError(
                        f'Unknown parent \'{parent_slug}\' for activity \'{slug}\'.'
                    )

                parent = self.slug_to_activity[parent_slug]
                self.activities_graph.add_edge(parent, child)

        self.validate()