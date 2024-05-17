import camelot
import sqlite3

def pdfs_to_csv():
    """Transforms the specifications pdfs into csvs""" 
    for f in ['eingang-entree-entrata_specifications', 'gebaeude-batiment-edificio_specifications', 'wohnung-logement-abitazione_specifications']:
        tables = camelot.read_pdf(f'./data/{f}.pdf', pages='all', flavor='stream')
        tables.export(f'./data/specificatinos/{f}.csv', f='csv')

def get_dimensions():
    """Identifies the dimension tables to be created""" 
    sqliteConnection = sqlite3.connect('./data/data.sqlite')
    cursor = sqliteConnection.cursor()
    cursor.execute('SELECT DISTINCT cmerkm FROM codes')
    data = cursor.fetchall()
    return list(sorted([code[0] for code in data]))

def build_join_with_all_dims_query(table, dims):
    """Builds a join with a fact and all its dimensional tables"""
    query = 'SELECT'
    query += '\n'
    for i, dim in enumerate(dims):
        query += f't{i}.{dim}_code'
        if i < len(dims) - 1:
            query += ','
        query += '\n'
    query += f'FROM fact_{table}'
    query += '\n'
    for i, dim in enumerate(dims):
        query += 'LEFT JOIN '
        query += f'dim_{dim} AS t{i} '        
        query += f'USING ({dim}_code)'
        query += f'\n'
    return query

if __name__ == '__main__':
    pass