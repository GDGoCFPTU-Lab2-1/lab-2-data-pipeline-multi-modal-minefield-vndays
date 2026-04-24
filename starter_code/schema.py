from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# v1 Schema - UnifiedDocument

class UnifiedDocument(BaseModel):
    document_id: str
    content: str
    source_type: str  # 'PDF', 'Video', 'HTML', 'CSV', 'Code'
    author: Optional[str] = "Unknown"
    timestamp: Optional[datetime] = None
    source_metadata: dict = Field(default_factory=dict)
