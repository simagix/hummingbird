// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	// FavIcon favicon
	FavIcon = `iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAAXNSR0IArs4c6QAAAnlJREFUOE+N0j1oFEEUAOD35mZ3Z3fvLnc5by+J2CRdQEFBGwvBQrCUoGIVC0EUxb/OxvgTsRBrQbuAqVKJCGpQGwtLQbAQiSJ7Ye/OO29/ZndnZ08mnHAkJziw1b753pv3HgIABYAMdh5zamrqkJTSbrVa7wAgGhMD6DjOPkqp67pubxSqVqsTUspFKeUMIj4KgsD7F3AriiJJKX2BiN+63W6ooEaj4fi+vyyE2FssFm90u92PACC2I6qCw/1+/46UckApfanr+nvbtjfSNNWDILifJMkxxtiaZVkPOp2OCwCDUQRnZ2cnPM87Hcfx9TzPK4SQn5TS14ZhvBFCnImi6CQh5Jdt20uI+Lzf76t+qY8rDAEA6/V6IwzDc0mSnJdSOojYp5R+KRQKJI7j/SqjqowxtiqEqBNCmrZtr3ue5ylAHTI5M7k7/h1fiHm8mOd5AwAkAOQAYKgAROSI+HswGOiI2DZNczkMw7W/wBYyPT29p9frXUyS5Gye59XhiEdjJCIGQ+BJGIaPR38qhFar1XnO+RUp5YEsy+ZVxmGVkhDiapq2qmnaBiFkl67rT7cDKlarVCozQogFzvlN1VhVfqFQUM1dsSxrJU3TxCyaR5jGNsYBCqkwxq4lSXIJEQWl9JVhGGu2bX/Y3NzsqJbMzc3VGGPlsUCxWKwLIe6laXqKUvq1VCpdZox9cl13a3TDJ6m7hbGA4zgN3/cfcs5PEEJ8xtiSZVnP2u22v2MTx+23qiDP89uc8wUAsHRdXzdN82qv1/u+YxPHAepSqVQ6mmXZcTUNTdM+l8vlu81m88f/AupprFarTRqGcZBSKjnnb1utVrA94R+3VhTEvu8HQAAAAABJRU5ErkJggg==`
	// LogoPNG hummingbird PNG
	LogoPNG = `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`
)

// Chart shows progress
type Chart struct {
	Title       string
	Completions [][2]interface{}
}

// StartWebServer start an http server at port 3629
func StartWebServer(port int) error {
	http.HandleFunc("/favicon.ico", faviconHandler)
	http.HandleFunc("/", gox.Cors(handler))
	addr := fmt.Sprintf(":%d", port)
	gox.GetLogger("StartWebServer").Infof("starting web server, http://localhost:%v", port)
	if err := http.ListenAndServe(addr, nil); err != nil {
		return fmt.Errorf("ListenAndServe failed: %v", err)
	}
	return nil
}

func faviconHandler(w http.ResponseWriter, r *http.Request) {
	r.Close = true
	r.Header.Set("Connection", "close")
	w.Header().Set("Content-Type", "image/x-icon")
	ico, _ := base64.StdEncoding.DecodeString(FavIcon)
	w.Write(ico)
}

func handler(w http.ResponseWriter, r *http.Request) {
	r.Close = true
	r.Header.Set("Connection", "close")
	if r.URL.Path == "/" {
		templ, err := GetHTMLTemplate()
		if err != nil {
			json.NewEncoder(w).Encode(bson.M{"ok": 0, "message": err})
			return
		}
		inst := GetMigratorInstance()
		ws := inst.Workspace()
		counts, err := ws.CountAllStatus()
		if err != nil {
			json.NewEncoder(w).Encode(bson.M{"ok": 0, "message": err.Error()})
		}
		total := counts.Added + counts.Completed + counts.Failed + counts.Processing + counts.Splitting
		percent := float64(counts.Completed) / float64(total)
		elapsed := time.Since(inst.genesis)
		millis := elapsed.Hours() + elapsed.Minutes() + elapsed.Seconds() + float64(elapsed.Milliseconds())
		remaining := time.Duration(time.Duration(millis*(1-percent)/percent) * time.Millisecond)
		eta := ""
		if counts.Splitting > 0 {
			eta = fmt.Sprintf("Splitting %v collection(s)", counts.Splitting)
		} else if percent == 1 {
			eta = "Initial data copy completed"
		} else {
			eta = fmt.Sprintf("Estimated %v (%.1f%%) remaining", remaining.Truncate(time.Second), (1-percent)*100)
		}
		completions := [][2]interface{}{[2]interface{}{"Status", "Total Counts"}}
		completions = append(completions, [2]interface{}{"Completed", counts.Completed})
		completions = append(completions, [2]interface{}{"Added", counts.Added})
		completions = append(completions, [2]interface{}{"Failed", counts.Failed})
		completions = append(completions, [2]interface{}{"Processing", counts.Processing})
		completions = append(completions, [2]interface{}{"Splitting", counts.Splitting})
		chart := Chart{Title: eta, Completions: completions}
	    w.Header().Set("Content-Type", "text/html")
		templ.Execute(w, chart)
	}
}

// GetHTMLTemplate returns HTML template
func GetHTMLTemplate() (*template.Template, error) {
	return template.New("neutrino").Funcs(template.FuncMap{
		"getLogo": func() string {
			return LogoPNG
		}}).Parse(HTMLTemplate)
}

// HTMLTemplate stores contents
const HTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <title>Ken Chen's HummingBird Project</title>
	<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
	<meta http-equiv="Pragma" content="no-cache" />
	<meta http-equiv="Expires" content="0" />
	<meta http-equiv="refresh" content="600">
  <script src="https://www.gstatic.com/charts/loader.js"></script>
  <link href="/favicon.ico" rel="icon" type="image/x-icon" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
  <style>
  	body {
		font-family: Helvetica, Arial, sans-serif;
		background-color: #f2f2f2;
		margin-top: 5px;
		margin-bottom: 10px;
		margin-right: 20px;
		margin-left: 20px;
  	}
    table
    {
    	font-family: Consolas, monaco, monospace;
    	border-collapse:collapse;
    	min-width:600px;
    }
    caption
    {
    	caption-side:top;
    	font-weight:bold;
    	font-style:italic;
    	margin:2px;
    }
    table, th, td
    {
		border: 1px solid gray;
		vertical-align: top;
    }
    th, td
    {
    	padding:2px;
    	vertical-align: top;
    }
    th
    {
      background-color: #ddd;
      font-weight:bold;
    }
    tr:nth-child(even) {background-color: #f2f2f2;}
    tr:nth-child(odd) {background-color: #fff;}
    .rowtitle
    {
    	font-weight:bold;
    }
	a {
	  text-decoration: none;
	  color: #000;
	  display: block;

	  -webkit-transition: font-size 0.3s ease, background-color 0.3s ease;
	  -moz-transition: font-size 0.3s ease, background-color 0.3s ease;
	  -o-transition: font-size 0.3s ease, background-color 0.3s ease;
	  -ms-transition: font-size 0.3s ease, background-color 0.3s ease;
	  transition: font-size 0.3s ease, background-color 0.3s ease;
	}
	a:hover {
	  color: blue;
	}
    .fixed {
      position: fixed;
      top: 20px;
      right: 20px;
    }
    h1 {
	  font-family: "Trebuchet MS";
      font-size: 1.7em;
      font-weight: bold;
    }
    h2 {
	  font-family: "Trebuchet MS";
      font-size: 1.5em;
      font-weight: bold;
    }
    h3 {
	  font-family: "Trebuchet MS";
      font-size: 1.25em;
      font-weight: bold;
    }
    h4 {
	  font-family: "Trebuchet MS";
      font-size: 1em;
      font-weight: bold;
	}
	.command {
	  background-color: #fff;
	  border: none;
	  outline:none;
	}
	.btn {
	  background-color: #fff;
	  border: none;
	  outline:none;
	  color: #4285F4;
	  padding: 5px 30px;
	  cursor: pointer;
	  font-size: 20px;
	}
	.btn:hover {
	  color: blue;
	  border: none;
	}
	.logo {
		display: flex;
		justify-content: center;
	}
	.chart_div {
	  display: flex;
	  justify-content: center;
	  margin: auto;
	  border: 5px solid #000;
	  padding: 5px;
	}
    </style>
</head>

<body>
<script type="text/javascript">
	function toggleDiv(tag) {
		var x = document.getElementById(tag);
		if (x.style.display === "none") {
	  		x.style.display = "block";
		} else {
	  		x.style.display = "none";
		}
  	}

	google.charts.load('current', {'packages':['corechart']});
  	google.charts.setOnLoadCallback(drawCharts);

  	function drawCharts(){
		try {
			drawGooglePieChart('progress', {{ .Title }}, {{ .Completions }}, 800, 600);
		} catch (err) {
		}
  	}

  	function drawGooglePieChart(divID, title, data, w, h) {
		if (data == null || data.length == 0) {
			return;
		}
    	var chart_data = new google.visualization.arrayToDataTable(data);
    	var options = {
			'title': title,
			'backgroundColor': '#f2f2f2',
      		'width': w,
      		'height': h,
  			'is3D': true,
  			'titleTextStyle': {'fontSize': 20},
  			'chartArea': {'width': '100%'},
  			'legend': {
				  'position': 'labeled', 
				  textStyle: {fontSize: 14 }
				}
  		};

    	// Instantiate and draw our chart, passing in some options.
    	var chart = new google.visualization.PieChart(document.getElementById(divID));
    	chart.draw(chart_data, options);
	}
</script>
</body>
	<div class='logo'><img src='data:image/png;base64, {{ getLogo }}'/></div>
	<div id="progress" class='chart_div'></div>
</html>
`
