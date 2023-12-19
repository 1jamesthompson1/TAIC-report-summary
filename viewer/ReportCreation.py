from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from io import BytesIO

class ReportGenerator:
    def __init__(self, search_result):
        self.search_result = search_result

    def generate(self):
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        p.drawString(100, 100, "testing")
        p.showPage()
        p.save()
        buffer.seek(0)
        return buffer