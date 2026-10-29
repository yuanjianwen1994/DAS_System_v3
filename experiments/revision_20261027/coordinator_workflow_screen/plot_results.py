import csv, json, statistics
from pathlib import Path
from reportlab.graphics.shapes import Drawing, String, Line, Rect
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.widgets.markers import makeMarker
from reportlab.lib.colors import HexColor
from reportlab.graphics import renderSVG

root=Path(__file__).resolve().parent
data=json.loads((root/'raw_1s.json').read_text())
rows=data['rows']
d=Drawing(800,450)
d.add(Rect(0,0,800,450,fillColor=HexColor('#FFFFFF'),strokeColor=None))
d.add(String(45,420,'Dependency-chain execution: measured EVM screen',fontSize=20,fontName='Helvetica-Bold'))
d.add(String(45,397,'1-second blocks; two workflow rounds; four common boundary operations; 3 fresh runs',fontSize=11))
lp=LinePlot();lp.x=80;lp.y=105;lp.width=590;lp.height=240
models=['DAS_LOCAL','ASYNC_RECEIPT_MODEL']
lp.data=[[(depth,statistics.mean(r['journey_seconds'] for r in rows if r['model']==m and r['depth']==depth)) for depth in [1,4,8]] for m in models]
lp.xValueAxis.valueMin=0;lp.xValueAxis.valueMax=9;lp.xValueAxis.valueSteps=[1,4,8]
lp.yValueAxis.valueMin=0;lp.yValueAxis.valueMax=22;lp.yValueAxis.valueStep=5
for i,color in enumerate(['#0072B2','#D55E00']):
 lp.lines[i].strokeColor=HexColor(color);lp.lines[i].strokeWidth=2.5
 lp.lines[i].symbol=makeMarker('FilledCircle');lp.lines[i].symbol.size=7;lp.lines[i].symbol.fillColor=HexColor(color)
d.add(lp)
d.add(String(280,70,'Number of dependent contract stages',fontSize=13))
d.add(String(15,360,'Journey (s)',fontSize=12))
for i,(color,label) in enumerate([('#0072B2','Local composition (DAS + two same-domain controls)'),('#D55E00','Sequential-confirmation asynchronous execution model')]):
 y=35-i*18;d.add(Line(80,y+3,105,y+3,strokeColor=HexColor(color),strokeWidth=3));d.add(String(115,y,label,fontSize=11))
d.add(String(45,375,'Same-domain Avalanche/Teleport controls use the same local execution path.',fontSize=10,fillColor=HexColor('#555555')))
renderSVG.drawToFile(d,str(root/'workflow_screen.svg'))
print(root/'workflow_screen.svg')
