from app.seed import import_sops, SOURCE_DIR


if __name__ == "__main__":
    count = import_sops()
    print(f"Imported/updated {count} SOPs from {SOURCE_DIR}")
