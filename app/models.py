from pydantic import BaseModel, Field
from typing import Optional, Any, List


class Device(BaseModel):
    mac: str
    name: Optional[str] = None
    bname: Optional[str] = None
    ipv4: Optional[str] = None
    ipv6: List[str] = []
    mac_vendor: Optional[str] = None
    local_domain: Optional[str] = None
    online: bool = False
    last_active: Optional[float] = None
    first_found: Optional[float] = None
    intf_uuid: Optional[str] = None
    spoofing: Optional[bool] = None
    open_ports: Optional[dict] = None
    extra: dict[str, Any] = Field(default_factory=dict)


class Policy(BaseModel):
    pid: Optional[str] = None
    action: Optional[str] = None
    type: Optional[str] = None
    target: Optional[str] = None
    target_name: Optional[str] = None
    target_ip: Optional[str] = None
    scope: Optional[Any] = None
    direction: Optional[str] = None
    protocol: Optional[str] = None
    local_port: Optional[str] = None
    activated_time: Optional[float] = None
    category: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class NetworkInfo(BaseModel):
    uuid: str
    data: dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    redis: str
    device_count: int
    timestamp: float
