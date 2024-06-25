import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
from sqlalchemy.sql import text



class Postgres:
    def __init__(self) -> None:
        self.engine = None

    
    def connect_postgres(self,INSTANCE_CONNECTION_NAME: str,DB_USER:str,DB_PASS:str,DB_NAME:str,PRIVATE_IP:bool=False) -> None:
        
        """
        Initializes a connection pool for a Cloud SQL instance of Postgres.

        Uses the Cloud SQL Python Connector package.
        """
        ip_type = IPTypes.PRIVATE if PRIVATE_IP else IPTypes.PUBLIC

        # initialize Cloud SQL Python Connector object
        connector = Connector()

        def _getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                INSTANCE_CONNECTION_NAME,
                "pg8000",
                user=DB_USER,
                password=DB_PASS,
                db=DB_NAME,
                ip_type=ip_type,
            )
            return conn

        # The Cloud SQL Python Connector can be used with SQLAlchemy
        # using the 'creator' argument to 'create_engine'
        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=_getconn,
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800,
        )
        return None

    def query_from_sql(self,sql:str):

        """
        args : 
            sql[str] : SQL query to be executed

        usage :
        
        ## init the class
            db = Postgres()
        ## connect to the database
            db.connect_postgres(INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS, DB_NAME, PRIVATE_IP)
        ## execute the query
            result = db.query_from_sql("SELECT * FROM table_name;")
        """

        query = text(sql)
        with self.engine.connect() as conn:
            result = conn.execute(query)
        return result.fetchall()