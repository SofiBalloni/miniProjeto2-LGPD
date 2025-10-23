from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, DateTime, text
from datetime import datetime
import csv
import re
import time
from functools import wraps

engine = create_engine("postgresql+psycopg2://alunos:AlunoFatec@174.138.65.214:5432/atividade2", echo=False)
metadata = MetaData()

usuarios = Table(
    'usuarios', metadata,
    Column('id', Integer, primary_key=True),
    Column('nome', String(50), nullable=False, index=True),
    Column('cpf', String(14), nullable=False),
    Column('email', String(100), nullable=False, unique=True),
    Column('telefone', String(20), nullable=False),
    Column('data_nascimento', Date, nullable=False),
    Column('created_on', DateTime(), default=datetime.now),
    Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
)

metadata.create_all(engine)

def log(msg):
    with open('lgpd_logs.txt', 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} | {msg}\n")

def medir_tempo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()
        r = func(*args, **kwargs)
        fim = time.perf_counter()
        dur = fim - inicio
        txt = f"{func.__name__} | {dur:.6f}s"
        print(txt)
        log(txt)
        return r
    return wrapper

def mask_nome(nome):
    partes = nome.split()
    if not partes:
        return nome
    p0 = partes[0]
    if len(p0) > 0:
        p0 = p0[0] + ("*" * max(0, len(p0) - 1))
    partes[0] = p0
    return " ".join(partes)

def only_digits(s):
    return re.sub(r"\D", "", s or "")

def mask_cpf(cpf):
    if not cpf:
        return cpf
    d = only_digits(cpf)
    if len(d) != 11:
        return cpf
    return f"{d[0:3]}.***.***-**"

def mask_email(email):
    if not email or "@" not in email:
        return email
    local, dom = email.split("@", 1)
    if not local:
        return email
    return local[0] + ("*" * max(0, len(local) - 1)) + "@" + dom

def mask_telefone(t):
    d = only_digits(t)
    return d[-4:] if len(d) >= 4 else d

def LGPD_row(row):
    return (
        row.id,
        mask_nome(row.nome),
        mask_cpf(row.cpf),
        mask_email(row.email),
        mask_telefone(row.telefone),
        row.data_nascimento,
        row.created_on,
        row.updated_on,
    )

@medir_tempo
def exportar_por_ano():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM usuarios;"))
        por_ano = {}
        for row in result:
            y = row.data_nascimento.year if row.data_nascimento else None
            if y is None:
                continue
            por_ano.setdefault(y, []).append(LGPD_row(row))
        for ano, linhas in por_ano.items():
            nome_arquivo = f"{ano}.csv"
            with open(nome_arquivo, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(["id","nome","cpf","email","telefone","data_nascimento","created_on","updated_on"])
                w.writerows(linhas)

@medir_tempo
def exportar_todos():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT nome, cpf FROM usuarios;"))
        with open("todos.csv", 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["nome","cpf"])
            for row in result:
                w.writerow([row.nome, row.cpf])

if __name__ == "__main__":
    users = []
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM usuarios LIMIT 10;"))
        for row in result:
            users.append(LGPD_row(row))
    for u in users:
        print(u)
    exportar_por_ano()
    exportar_todos()
