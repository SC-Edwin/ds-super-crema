"""업로드 도메인 유스케이스 (Streamlit `st` 최소화).

점진적으로 `ui/upload_tab.py` 등에서 호출만 하도록 옮깁니다.
현재는 HTTP 경계가 `network`/`service`에 있고, 화면·흐름은 대부분 `ui/`에 있습니다.
"""
