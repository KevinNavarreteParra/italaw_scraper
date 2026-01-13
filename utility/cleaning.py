import pandas as pd
import re


def flatten_to_document_level(data_list):
    """
    Flattens the arbitration-level data list to arbitration-document level.
    
    Returns:
        pd.DataFrame
    """
    flattened_rows = []

    for entry in data_list:
        base = {k: v for k, v in entry.items() if k != 'documents'}
        documents = entry.get('documents', [])

        if documents:
            for doc in documents:
                row = base.copy()
                row['doc_date'] = doc.get('date')
                row['doc_name'] = doc.get('doc_name')
                row['doc_link'] = doc.get('doc_link')

                # Flatten details into individual columns (optional: keep nested if preferred)
                details = doc.get('details', {})
                for key, value in details.items():
                    row[f'detail_{key}'] = value

                flattened_rows.append(row)
        else:
            # Arbitration with no documents
            row = base.copy()
            row['doc_date'] = None
            row['doc_name'] = None
            row['doc_link'] = None
            flattened_rows.append(row)

    return pd.DataFrame(flattened_rows)


def to_snake_case(text):
    # Lowercase
    text = text.lower()
    # Replace non-alphanumeric with underscores
    text = re.sub(r'[^a-z0-9]+', '_', text)
    # Remove leading/trailing underscores
    text = text.strip('_')
    return text