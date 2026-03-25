import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class Race(Base):
    __tablename__ = 'races'
    
    id = Column(String, primary_key=True)  # netkeibaのレースID (例: 202405020111)
    date = Column(Date, nullable=False)
    place = Column(String, nullable=False)  # 競馬場 (東京、中山など)
    race_number = Column(Integer, nullable=False)  # レース番号 (1R, 11Rなど)
    race_name = Column(String)  # レース名 (日本ダービーなど)
    course_type = Column(String)  # 芝/ダート
    distance = Column(Integer)  # 距離
    weather = Column(String)  # 天候
    track_condition = Column(String)  # 馬場状態
    
    results = relationship("Result", back_populates="race")

class Horse(Base):
    __tablename__ = 'horses'
    
    id = Column(String, primary_key=True)  # netkeibaの馬ID
    name = Column(String, nullable=False)
    sire = Column(String)  # 父
    dam = Column(String)  # 母
    dam_sire = Column(String)  # 母父
    
    results = relationship("Result", back_populates="horse")

class Result(Base):
    __tablename__ = 'results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(String, ForeignKey('races.id'))
    horse_id = Column(String, ForeignKey('horses.id'))
    
    rank = Column(Integer)  # 着順 (失格などは別処理が必要な場合あり)
    horse_number = Column(Integer)  # 馬番
    jockey = Column(String)  # 騎手
    weight_carried = Column(Float)  # 斤量
    time_seconds = Column(Float)  # タイム（秒換算）
    margin = Column(String)  # 着差
    odds = Column(Float)  # 単勝オッズ
    popularity = Column(Integer)  # 人気
    last_3f = Column(Float)  # 上がり3ハロン
    horse_weight = Column(String)  # 馬体重
    
    race = relationship("Race", back_populates="results")
    horse = relationship("Horse", back_populates="results")

class HorseAbility(Base):
    """
    馬の能力値を保存するテーブル
    """
    __tablename__ = 'horse_abilities'
    
    horse_id = Column(String, ForeignKey('horses.id'), primary_key=True)
    speed = Column(Float, default=0.0) # スピード
    stamina = Column(Float, default=0.0) # スタミナ
    explosiveness = Column(Float, default=0.0) # 瞬発力
    consistency = Column(Float, default=0.0) # 安定性
    experience = Column(Float, default=0.0) # 実績
    last_updated = Column(Date)
    
    horse = relationship("Horse", backref="ability")

class JockeyAbility(Base):
    """
    騎手の能力値(実績)を保存するテーブル
    """
    __tablename__ = 'jockey_abilities'
    
    jockey_name = Column(String, primary_key=True) 
    score = Column(Float, default=0.0)      # 総合スコア (Max 100相当)
    win_rate = Column(Float, default=0.0)   # 勝率
    top3_rate = Column(Float, default=0.0)  # 複勝率
    races_run = Column(Integer, default=0)  # 騎乗回数 (サンプル数として)
    last_updated = Column(Date)

def get_engine(db_path='sqlite:///data/keiba.db'):
    """データベースエンジンを取得する。"""
    engine = create_engine(db_path, echo=False)
    return engine

def init_db(engine):
    """テーブルを作成する。"""
    Base.metadata.create_all(engine)

def get_session(engine):
    """セッションを作成する。"""
    Session = sessionmaker(bind=engine)
    return Session()

if __name__ == '__main__':
    # スクリプトとして直接実行された場合はDBを初期化
    engine = get_engine()
    init_db(engine)
    print("データベースが初期化されました。")
