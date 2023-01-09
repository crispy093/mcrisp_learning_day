from sqlalchemy import ForeignKey, create_engine, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import Column, Integer, String
from sqlalchemy.sql import exists

# Create an in-memory SQLite db
engine = create_engine("sqlite:///:memory:", echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s'>" % (
            self.name,
            self.fullname,
        )


class Address(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"))

    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return "<Address(email_address='%s')>" % self.email_address


User.addresses = relationship("Address", order_by=Address.id, back_populates="user")


def relational_data(session):
    print("Relational data")
    session.add_all(users_data())
    session.commit()

    wendy = session.query(User).filter(User.name == "wendy").first()
    print(wendy)
    print(wendy.addresses)

    wendy.addresses = [
        Address(email_address="wendy@oda.com"),
        Address(email_address="wendy@odascompetitor.com"),
    ]

    print(wendy.addresses)

    address = session.query(Address).first()

    print(address.user)


def _joins(session):
    print("Joins")
    wendy = (
        session.query(User)
        .join(Address)
        .filter(Address.email_address == "wendy@oda.com")
    ).first()
    print(wendy)


def _subqueries(session):
    print("Subqueries")
    stmt = (
        session.query(Address.user_id, func.count("*").label("address_count"))
        .group_by(Address.user_id)
        .subquery()
    )

    for u, count in (
        session.query(User, stmt.c.address_count)
        .outerjoin(stmt, User.id == stmt.c.user_id)
        .order_by(stmt.c.address_count)
    ):
        print(u, count)


def _exists_any_has(session):
    stmt = exists().where(Address.user_id == User.id)

    for (name,) in session.query(User.name).filter(stmt):
        print(name)

    for (name,) in session.query(User.name).filter(
        User.addresses.any(Address.email_address.like("%oda.com%"))
    ):
        print(name)

    print(session.query(Address).filter(~Address.user.has(User.name == "wendy")).all())


def users_data():
    return [
        User(name="wendy", fullname="Wendy Williams"),
        User(name="mary", fullname="Mary Contrary"),
        User(name="fred", fullname="Fred Flintstone"),
    ]


Base.metadata.create_all(engine)

relational_data(session=Session())
print()
_joins(session=Session())
print()
_subqueries(session=Session())
print()
_exists_any_has(session=Session())
