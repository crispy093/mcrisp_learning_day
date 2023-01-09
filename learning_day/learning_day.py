from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String

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


Base.metadata.create_all(engine)


def do_queries_get_data_staged_in_transactions(session):
    print("Do queries get data from staged transactions in addition to the DB?")
    # session = Session()
    session.query(User).delete()

    users = session.query(User).all()
    print(f"Initial users: {get_users_names(users)}")

    session.add_all(users_data())

    # Query object isn't updated
    users_pre_save = session.query(User).all()
    print(f"Users before commiting to db: {get_users_names(users_pre_save)}")

    # Save to database
    session.commit()
    users_post_save = session.query(User).all()
    print(f"Users after committing to db: {get_users_names(users_post_save)}")

    print("Yes, they do!")


def does_it_do_the_same_with_filter(session):
    print("Does it do the same for filtering?")
    # session = Session()
    session.query(User).delete()

    wendy = session.query(User).filter(User.name == "wendy").all()
    print(f"Initial search for Wendy: {wendy}")

    session.add_all(users_data())

    wendy_pre_save = session.query(User).filter(User.name == "wendy").all()
    print(f"Search for Wendy before saving: {wendy_pre_save}")

    session.commit()
    wendy_post_save = session.query(User).filter(User.name == "wendy").all()
    print(f"Search for Wendy after saving: {wendy_post_save}")

    print("Yep!")


def how_does_it_do_that(session):
    print("How does it do that?")
    session.query(User).delete()

    session.add_all(users_data())
    print(f"session.new: {session.new}")
    print(
        "^^^This is the variable that holds what data is to be saved on the next commit"
    )
    print("And when we run a filter and try to retrieve the objects...")
    users = session.query(User).filter(User.name == "wendy").all()
    print(f"session.new: {session.new}")
    print(
        "This variable is now empty - it commits all transactions before retrieving new data"
    )


def users_data():
    return [
        User(name="wendy", fullname="Wendy Williams"),
        User(name="mary", fullname="Mary Contrary"),
        User(name="fred", fullname="Fred Flintstone"),
    ]


def get_users_names(query):
    users: list[str] = []

    for user in query:
        users.append(user.name)

    return users


do_queries_get_data_staged_in_transactions(session=Session())
print()
does_it_do_the_same_with_filter(session=Session())
print()
how_does_it_do_that(session=Session())
