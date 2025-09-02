from typing import Dict, List

class EventStore:
    """In-memory Event Store последних событий пользователя."""

    def __init__(self, max_events_per_user: int = 50) -> None:
        self.events: Dict[int, List[int]] = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id: int, track_id: int) -> None:
        lst = self.events.get(user_id, [])
        self.events[user_id] = [track_id] + lst[: self.max_events_per_user - 1]

    def get(self, user_id: int, k: int = 10) -> List[int]:
        return self.events.get(user_id, [])[:k]
