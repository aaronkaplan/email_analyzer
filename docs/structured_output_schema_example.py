from pydantic import BaseModel, Field


class mySchema(BaseModel):
    sender: str = Field(..., alias="from", description="Email sender")
    recipient: str = Field(description="Email recipient")
    subject: str = Field(description="Email subject line")
    date: str = Field(description="Date the email was sent")
    category: str = Field(description="High-level email category")
    priority: str = Field(description="Priority label such as low, medium, or high")
    summary: str = Field(description="One short summary of the email")
    action_items: list[str] = Field(default_factory=list, description="Concrete requested follow-up actions")
    is_spam: bool = Field(description="Whether the email is likely to be spam")
    is_ham: bool = Field(description="Whether the email is likely to be ham (not spam)")
    spam_score: float = Field(description="A score from 0.0 to 5.0 indicating the likelihood of the email being spam")