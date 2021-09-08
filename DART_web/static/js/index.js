	//주요 전역변수들
	var infoCSV = null;
	var stockCSV = null;
	var newsCountChart = null; //뉴스수 차트
	var discussionCountChart = null; //종토방 글 수 차트
	var stockChart = null; //주가 차트
    var PERChart = null; //PER차트
    var PBRChart = null;//PBR차트
    var growthChart = null; //기업의 성장성 차트
	var earningChart = null; //기업의 수익성 차트
	var stabilityChart = null; //기업의 안정성차트
	var activityChart = null; //기업의 활동성차트
    

	/*CSV관련 처리 부분*/
	function parseCSV(csv) {
		const csvLines = csv.split(/\r\n|\n/); //줄 하나하나를 csvLines로 정의함
		var output = {};
		const columns = csvLines.shift().split(','); //0번째 줄을 columns으로 정의

		//각 줄을 columns의 요소와 매핑후 리턴
		columns.forEach(function(val, idx, arr){
			var tempArr=[];
			csvLines.forEach(function(value, index, array){
				var inputRow = value.split(',');
				tempArr.push(inputRow[idx]);
			});
		output[val]=tempArr;
	});
	return output;
	}

	//음봉인지 양봉인지?
	var risingColor = "red";
	var fallingColor = "blue";
	function getCandleColor(open, close){
		if (open > close){
			return fallingColor;
		}else if(open < close){
			return risingColor;
		}else{
			return "black";
		}
	}

	/*주식 데이터를얻기*/
	//주가정보 함수
	function getStockDataByName(stockName){
		var firstIndex = stockCSV["NAME"].indexOf(stockName);
		var lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		var output = [];

		for(let i=firstIndex; i<=lastIndex; i++){
			let tempRow = {
				x: new Date(stockCSV["DATE"][i]),
				y:[ // Y: [Open, High ,Low, Close]
				Number(stockCSV["OPEN"][i]),
				Number(stockCSV["HIGH"][i]),
				Number(stockCSV["LOW"][i]),
				Number(stockCSV["CLOSE"][i]),
				],
				color: getCandleColor(Number(stockCSV["OPEN"][i]), Number(stockCSV["CLOSE"][i]))
			};
			output.push(tempRow);
		}
		return output;
	}
	//주식 정보를 얻기 (기준: 매일)
	function getStockDataByKey(stockName, key, number=true){
		var firstIndex = stockCSV["NAME"].indexOf(stockName);
		var lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		var output = [];
		for(let i=firstIndex; i<=lastIndex; i++){
			if(number){
				y = Number(stockCSV[key][i]);
			}else{
				y = stockCSV[key][i];
			}
            if (key.startsWith("week")){
                let weeks = Number(key.substring(4,5));
                let millisBefore = weeks * 7 * 86400000;
                output.push({
                    x: new Date(new Date(stockCSV["DATE"][i]) - millisBefore),
                    y: y
                });
            }else{
                output.push({
                    x: new Date(stockCSV["DATE"][i]),
                    y: y
                })
        };
		}
		return output;
	}
	//주식 정보를 얻기 (기준: 분기)
	function getStockDataInQuarters(stockName, category){
		let firstIndex = stockCSV["NAME"].indexOf(stockName);
		let lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		let dateData = stockCSV["DATE"].slice(firstIndex, lastIndex+1);
		let months = ["02", "05", "08", "11"];
		let presentYear = new Date().getFullYear();
		let output = {};
		for(let year=2018; year<presentYear; year++){
			let searchFor = months.map(value => value = year + "-" + value);
			let indexesFound = searchFor.map(value => dateData.findIndex(v => v.startsWith(value)));
			indexesFound.forEach((value, index) => {
				let keyStr = year + "`" + (index+1) + "Q"
				if (value != -1){
					output[keyStr] = stockCSV[category][firstIndex + value];
				}else{
					output[keyStr] = null;
				}
			});
		}
		return output;
	}
	//주식 정보를 얻기 (최근 4주, 비정형전용)
	function getDataLast4Weeks(stockName, type){
		let lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		let labels = getLabelLast4Weeks(stockName);
		let week4 = labels[0];
		let week3 = labels[1];
		let week2 = labels[2];
		let week1 = labels[3];
		//let output = {};
		switch(type){
			case "news_count": 
				/*
				output[week4] = stockCSV["week4_기사_count"][lastIndex];
				output[week3] = stockCSV["week3_기사_count"][lastIndex];
				output[week2] = stockCSV["week2_기사_count"][lastIndex];
				output[week1] = stockCSV["week1_기사_count"][lastIndex];
				*/
				output = [stockCSV["week4_기사_count"][lastIndex], stockCSV["week3_기사_count"][lastIndex], stockCSV["week2_기사_count"][lastIndex], stockCSV["week1_기사_count"][lastIndex]]
				return output;
			case "discussion_count":
				/*
				output[week4] = stockCSV["week4_종토방_글수"][lastIndex];
				output[week3] = stockCSV["week3_종토방_글수"][lastIndex];
				output[week2] = stockCSV["week2_종토방_글수"][lastIndex];
				output[week1] = stockCSV["week1_종토방_글수"][lastIndex];
				*/
				output = [stockCSV["week4_종토방_글수"][lastIndex], stockCSV["week3_종토방_글수"][lastIndex], stockCSV["week2_종토방_글수"][lastIndex], stockCSV["week1_종토방_글수"][lastIndex]];
				return output;
		}
	}
	function getLabelLast4Weeks(stockName){
		let lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		let date = new Date(stockCSV["DATE"][lastIndex]);
		let week4 = new Date(date - (28 * 86400000)).toLocaleString("ko-KR").substring(0,12);
		let week3 = new Date(date - (21 * 86400000)).toLocaleString("ko-KR").substring(0,12);
		let week2 = new Date(date - (14 * 86400000)).toLocaleString("ko-KR").substring(0,12);
		let week1 = new Date(date - (7 * 86400000)).toLocaleString("ko-KR").substring(0,12);
		return [week4, week3, week2, week1];
	}
	function updateLabel(stockName){
		let lastIndex = stockCSV["NAME"].lastIndexOf(stockName);
		let label = new Number(stockCSV["label"][lastIndex]);
		let date = stockCSV["DATE"][lastIndex];
		let avg = 0;
		//제목 업데이트
		$("#stock-name").html(stockName);
		
		//라벨 업데이트
		$("#label-date").val("라벨 값: " + label + " (" + date +" 기준)");
		if (label < 0){
			$("#stock-grade").html("C");
		}else if(0 <= label && label <= 0.1){
			$("#stock-grade").html("B");
		}else{
			$("#stock-grade").html("A");
		}
		$("#stock-grade-bottom").html("등급");

		//시총과 섹터 업데이트
		let cap = stockCSV["CAPITALIZATION"][lastIndex];
		let capstr = "";
		if (cap >= 1000000000000){
			capstr = Math.floor(cap / 1000000000000) + "조";
			if(Math.floor(cap % 1000000000000 / 100000000) >= 1) capstr+= Math.floor(cap % 1000000000000 / 100000000) + "억";
		}else{
			capstr = Math.floor(cap / 100000000) + "억";
		}
		capstr += "원";
		capstr += " (" + date + " 기준)";
		$("#capitalization").html(capstr);
		$("#main-business").html(stockCSV["SECTOR"][lastIndex]);

		//뉴스 긍정도 업데이트
		let dateWeekBefore = new Date(new Date(date) - (86400000*28));
		let dateWeekBeforeStr = dateWeekBefore.getFullYear() + "-" + (dateWeekBefore.getMonth()+1) + "-" + dateWeekBefore.getDate();
		avg = (
			((Number(stockCSV["week1_기사_긍부정평균"][lastIndex]) + 1) / 2) +
			((Number(stockCSV["week2_기사_긍부정평균"][lastIndex]) + 1) / 2) +
			((Number(stockCSV["week3_기사_긍부정평균"][lastIndex]) + 1) / 2) +
			((Number(stockCSV["week4_기사_긍부정평균"][lastIndex]) + 1) / 2)
			) / 4 * 100;
			$("#news-date").val(Math.round(avg*10)/10 + "% (" + dateWeekBeforeStr+ " ~ " + date + ")");
			$("#news-range").val(avg);
		//종토방 긍정도 업데이트
		avg = (
			Number(stockCSV["week1_종토방_긍부정평균"][lastIndex]) + 
			Number(stockCSV["week2_종토방_긍부정평균"][lastIndex]) + 
			Number(stockCSV["week3_종토방_긍부정평균"][lastIndex]) + 
			Number(stockCSV["week4_종토방_긍부정평균"][lastIndex])
			) / 4 * 100;
			$("#discussion-date").val(Math.round(avg*10)/10 + "% (" + dateWeekBeforeStr+ " ~ " + date + ")");
			$("#discussion-range").val(avg);

		//종목 기본정보 업데이트
		let infoIndex = null;
		try {
			infoIndex = infoCSV["종목명"].indexOf(stockName);
			$("#per-value").html(infoCSV["액면가"][infoIndex]);
			$("#stock-count").html(infoCSV["주식수"][infoIndex]);
			if(infoCSV["wordcloud_news_positive"][infoIndex] != null && infoCSV["wordcloud_news_positive"][infoIndex] != ""){
				$("#wordcloud-positive-img").attr("src", "../static/img/" + infoCSV["wordcloud_news_positive"][infoIndex]);
			}else{ 
				$("#wordcloud-positive-img").removeAttr("src"); 
			}
		} catch (error) {
			console.log(error);
			$("#per-value").html("-");
			$("#stock-count").html("-");
		}
	}

window.onload = function () {

	/*CSV 로드*/
	$.ajax({
		type: "GET",
		url: "stock_info.csv",
		dataType: "text",
		success: function(data) {
			infoCSV = parseCSV(data);
			}
		});
	$.ajax({
		type: "GET",
		url: "final_with_label.csv",
		dataType: "text",
		success: function(data) {
			stockCSV = parseCSV(data);
			chartInit("LG화학");
			eventListners();
			}
		});
	
	/*차트 초기화*/
	function chartInit(stockName){
		//stockChart 부분
		stockChart = new CanvasJS.StockChart("chartContainer",{
		animationEnabled: true,
		animationDuration: 1000,
		theme: "light2",
		exportEnabled: false,
		title:{
		text: stockName + " 주가",
		fontSize: 32
		},
		charts: [{	
			toolTip:{
				shared: true,
			},
			axisX: {
        		crosshair: {
          		enabled: true,
          		snapToDataPoint: true
        		}
			},
			//주가 차트
			data: [{
				name:"주가",
				type: "candlestick",
				risingColor: risingColor,
				fallingColor: fallingColor,
				yValueFormatString: "###,###,###",
				dataPoints : getStockDataByName(stockName),
			},
			{
				type: "line",
				color: "#00c50d",
				lineThickness: 1,
				showInLegend: true,
				name: "MA5",
				dataPoints: getStockDataByKey(stockName, "MA5")
			},
			{
				type: "line",
				color: "#ff333a",
				lineThickness: 1,
				showInLegend: true,
				name: "MA10",
				dataPoints: getStockDataByKey(stockName, "MA10")
			},
			{
				type: "line",
				color: "#f48416",
				lineThickness: 1,
				showInLegend: true,
				name: "MA20",
				dataPoints: getStockDataByKey(stockName, "MA20")
			},
			{
				type: "line",
				color: "#892dff",
				lineThickness: 1,
				showInLegend: true,
				name: "MA50",
				dataPoints: getStockDataByKey(stockName, "MA50")
			}]
		},
		{
			toolTip:{
				shared: true,
			},
			axisX: {
        		crosshair: {
          		enabled: true,
          		snapToDataPoint: true
        		}
			},
			// 외국인, 기관, 법인, 개인 매수거래금액 추이
			data:[
			{
				type: "stackedColumn",
				color: "blue",
				lineThickness: 1,
				showInLegend: true,
				name: "기관 순매수금",
				dataPoints: getStockDataByKey(stockName, "INSTITUTION(NP)")
			},
			
			{
				type: "stackedColumn",
				color: "purple",
				lineThickness: 1,
				showInLegend: true,
				name: "법인 순매수금",
				dataPoints: getStockDataByKey(stockName, "CORP(NP)")
			},
			{
				type: "stackedColumn",
				color: "red",
				lineThickness: 1,
				showInLegend: true,
				name: "개인 순매수금",
				dataPoints: getStockDataByKey(stockName, "INDIVIDUAL(NP)")
			},
			{
				type: "stackedColumn",
				color: "green",
				lineThickness: 1,
				showInLegend: true,
				name: "외국인 순매수금",
				dataPoints: getStockDataByKey(stockName, "FOREIGN(NP)")
			}
			]
		}],
		navigator: {
			data: [{
				color: "#6D78AD",
				name: "volume",
				dataPoints: getStockDataByKey(stockName, "VOLUME")
			}],
			slider: {
				minimum: new Date(2020, 07, 01),
				maximum: new Date(2020, 12, 30)
			}
		},
		rangeSelector:{
			inputFields:{
				enabled: false
			},
			buttonStyle:{
				labelFontSize:12,
				width: 50,
			}
		},

		});
		stockChart.render();

	/*재무비율 차트 부분*/
	//PER차트 - stockPERChart
	PERChart = new Chart("stockPERChart", {
		type: 'line',
		data: {
			datasets: [{
				label: 'PER',
				data: getStockDataInQuarters(stockName, "PER"),
				backgroundColor: [
					'rgba(255, 99, 132, 0.2)',
				],
				borderColor: [
					'rgba(255, 99, 132, 1)',
				],
				borderWidth: 1
			}]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: 'PER ratio',
				}
			}
		}
	});
	//PBR차트 - stockPBRChart
	PBRChart = new Chart("stockPBRChart", {
		type: 'line',
		data: {
			datasets: [{
				label: 'PBR',
				data: getStockDataInQuarters(stockName, "PBR"),
				backgroundColor: [
					'rgba(54, 162, 235, 0.2)',
				],
				borderColor: [
					'rgba(54, 162, 235, 1)',
				],
				borderWidth: 1
			}]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: 'PBR ratio',
				}
			}
		}
	});
	
	//재무비율(성장성) 차트 - financialGrowth
	growthChart = new Chart("financialGrowth", {
		type: 'bar',
		data: {
			datasets: [{
				label: '총자산 증가율',
				data: getStockDataInQuarters(stockName, "ASST_INC"),
				backgroundColor: [
					'rgba(255, 99, 132, 0.2)',
				],
				borderColor: [
					'rgba(255, 99, 132, 1)',
				],
				borderWidth: 1
			},{
				label: '매출액 증가율',
				data: getStockDataInQuarters(stockName, "REV_INC"),
				backgroundColor: [
					'rgba(255, 206, 86, 0.2)',
				],
				borderColor: [
					'rgba(255, 206, 86, 1)',
				],
				borderWidth: 1
			},{
				label: '당기순이익 증가율',
				data: getStockDataInQuarters(stockName, "PROF_INC"),
				backgroundColor: [
					'rgba(54, 162, 235, 0.2)',
				],
				borderColor: [
					'rgba(54, 162, 235, 1)',
				],
				borderWidth: 1
			}/*,{
				label: '자기자본',
				data: getStockDataInQuarters(stockName, "S_ASST_INC"),
				backgroundColor: [
					'rgba(161, 252, 3, 0.2)',
				],
				borderColor: [
					'rgba(161, 252, 3, 1)',
				],
				borderWidth: 1
			}*/]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: '재무비율(성장성)',
				}
			}
		}
	});
	//재무비율(수익성) 차트 - financialEarning
	earningChart = new Chart("financialEarning",{
		type: 'bar',
		data: {
			datasets: [{
				label: '매출액 영업이익률',
				data: getStockDataInQuarters(stockName, "REV_BPR"),
				backgroundColor: [
					'rgba(255, 99, 132, 0.2)',
				],
				borderColor: [
					'rgba(255, 99, 132, 1)',
				],
				borderWidth: 1
			},/*{
				label: '매출액 순이익률',
				data: getStockDataInQuarters(stockName, "REV_NPR"),
				backgroundColor: [
					'rgba(255, 206, 86, 0.2)',
				],
				borderColor: [
					'rgba(255, 206, 86, 1)',
				],
				borderWidth: 1
			},*/{
				label: '총자본 순이익률',
				data: getStockDataInQuarters(stockName, "EQ_NPR"),
				backgroundColor: [
					'rgba(54, 162, 235, 0.2)',
				],
				borderColor: [
					'rgba(54, 162, 235, 1)',
				],
				borderWidth: 1
			},{
				label: '경영자산 영업이익률',
				data: getStockDataInQuarters(stockName, "RA_BPR"),
				backgroundColor: [
					'rgba(161, 252, 3, 0.2)',
				],
				borderColor: [
					'rgba(161, 252, 3, 1)',
				],
				borderWidth: 1
			}]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: '재무비율(수익성)',
				}
			}
		}
	});


	//재무비율(안정성) 차트 - financialStability
	
	stabilityChart = new Chart("financialStability",{
		type: 'bar',
		data: {
			datasets: [{
				label: '유동비율',
				data: getStockDataInQuarters(stockName, "R_RATIO"),
				backgroundColor: [
					'rgba(255, 99, 132, 0.2)',
				],
				borderColor: [
					'rgba(255, 99, 132, 1)',
				],
				borderWidth: 1
			},{
				label: '당좌비율',
				data: getStockDataInQuarters(stockName, "D_RATIO"),
				backgroundColor: [
					'rgba(255, 206, 86, 0.2)',
				],
				borderColor: [
					'rgba(255, 206, 86, 1)',
				],
				borderWidth: 1
			},{
				label: '고정비율',
				data: getStockDataInQuarters(stockName, "F_RATIO"),
				backgroundColor: [
					'rgba(54, 162, 235, 0.2)',
				],
				borderColor: [
					'rgba(54, 162, 235, 1)',
				],
				borderWidth: 1
			},{
				label: '부채비율',
				data: getStockDataInQuarters(stockName, "DEBT_R"),
				backgroundColor: [
					'rgba(161, 252, 3, 0.2)',
				],
				borderColor: [
					'rgba(161, 252, 3, 1)',
				],
				borderWidth: 1
			}]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: '재무비율(안정성)',
				}
			}
		}
	});
	

	//재무비율(활동성) 차트 - financialActivity
	activityChart = new Chart("financialActivity",{
		type: 'bar',
		data: {
			datasets: [{
				label: '총자본 회전율',
				data: getStockDataInQuarters(stockName, "ASST_TO"),
				backgroundColor: [
					'rgba(255, 99, 132, 0.2)',
				],
				borderColor: [
					'rgba(255, 99, 132, 1)',
				],
				borderWidth: 1
			},{
				label: '매출채권 회전율',
				data: getStockDataInQuarters(stockName, "SA_C_TO"),
				backgroundColor: [
					'rgba(255, 206, 86, 0.2)',
				],
				borderColor: [
					'rgba(255, 206, 86, 1)',
				],
				borderWidth: 1
			},{
				label: '재고자산 회전율',
				data: getStockDataInQuarters(stockName, "ST_TO"),
				backgroundColor: [
					'rgba(54, 162, 235, 0.2)',
				],
				borderColor: [
					'rgba(54, 162, 235, 1)',
				],
				borderWidth: 1
			}]
		},
		options: {
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins:{
				title:{
					display:true,
					text: '재무비율(활동성)',
				}
			}
		}
	});

	/* 상단의 회사 등급 및 회사 설명
		그리고 비정형 분석결과 업데이트
	 */
	updateLabel(stockName);
	
	//뉴스 수 차트
	newsCountChart=  new Chart('news-count',{
		type: 'line',
		data:{
			labels: getLabelLast4Weeks(stockName),
			datasets:[{
				label: '뉴스 기사 수',
				data: getDataLast4Weeks(stockName, "news_count"),
				backgroundColor:[
					'rgba(54, 162, 235, 0.2)'
				],
				borderColor:[
					'rgba(54, 162, 235, 1)'
				]
			}]
		},
		options:{
			scales: {
				y: {
					beginAtZero:true,
				}
			},
			plugins:{
				title:{
					display: true,
					text: '뉴스 기사 수 (4주간)'
				}
			}
		}
	});

	//종토방 글 수 차트
	discussionCountChart =  new Chart('discussion-count',{
		type: 'line',
		data:{
			labels: getLabelLast4Weeks(stockName),
			datasets:[{
				label: '종목토론방 글 수',
				data: getDataLast4Weeks(stockName, "discussion_count"),
				backgroundColor:[
					'rgba(161, 252, 3, 0.2)'
				],
				borderColor:[
					'rgba(161, 252, 3, 1)'
				]
			}]
		},
		options:{
			scales: {
				y: {
					beginAtZero:true,
				}
			},
			plugins:{
				title:{
					display: true,
					text: '종목토론방 글 수 (4주간)'
				}
			}
		}
	});
}
}

//버튼에 이벤트리스너 달아주기
function eventListners(){
    $("#updateStockName").click(function(){
        let updateTo = $("#stockNameInput").val();

        if(stockCSV["NAME"].indexOf(updateTo) == -1){

            alert(updateTo + "은(는) 없습니다");

        }else if(updateTo == ""){

			$("#stockNameInput").focus();
			alert("종목 이름을 입력하세요");

		}else{
            stockChart.options.title.text = updateTo + " 주가";

            stockChart.options.charts[0].data[0].dataPoints = getStockDataByName(updateTo);
            stockChart.options.charts[0].data[1].dataPoints = getStockDataByKey(updateTo, "MA5");
            stockChart.options.charts[0].data[2].dataPoints = getStockDataByKey(updateTo, "MA10");
            stockChart.options.charts[0].data[3].dataPoints = getStockDataByKey(updateTo, "MA20");
            stockChart.options.charts[0].data[4].dataPoints = getStockDataByKey(updateTo, "MA50");

            stockChart.options.charts[1].data[0].dataPoints = getStockDataByKey(updateTo, "INSTITUTION(NP)");
            stockChart.options.charts[1].data[1].dataPoints = getStockDataByKey(updateTo, "CORP(NP)");
            stockChart.options.charts[1].data[2].dataPoints = getStockDataByKey(updateTo, "INDIVIDUAL(NP)");
            stockChart.options.charts[1].data[3].dataPoints = getStockDataByKey(updateTo, "FOREIGN(NP)");
            stockChart.options.navigator.data[0].dataPoints = getStockDataByKey(updateTo, "VOLUME");

            stockChart.render();

			PERChart.data.datasets[0].data = getStockDataInQuarters(updateTo, "PER");
			PERChart.update()

			PBRChart.data.datasets[0].data = getStockDataInQuarters(updateTo, "PBR");
			PBRChart.update()

			growthChart.data.datasets[0].data =getStockDataInQuarters(updateTo, "ASST_INC");
			growthChart.data.datasets[1].data =getStockDataInQuarters(updateTo, "REV_INC");
			growthChart.data.datasets[2].data =getStockDataInQuarters(updateTo, "PROF_INC");
			//growthChart.data.datasets[2].data =getStockDataInQuarters(updateTo, "S_ASST_INC");
			growthChart.update();

			earningChart.data.datasets[0].data = getStockDataInQuarters(updateTo, "REV_BPR");
			//earningChart.data.datasets[1].data = getStockDataInQuarters(updateTo, "REV_NPR");
			earningChart.data.datasets[1].data = getStockDataInQuarters(updateTo, "EQ_NPR");
			earningChart.data.datasets[2].data = getStockDataInQuarters(updateTo, "RA_BPR");
			earningChart.update();

			stabilityChart.data.datasets[0].data = getStockDataInQuarters(updateTo, "R_RATIO");
			stabilityChart.data.datasets[1].data = getStockDataInQuarters(updateTo, "D_RATIO");
			stabilityChart.data.datasets[2].data = getStockDataInQuarters(updateTo, "F_RATIO");
			stabilityChart.data.datasets[3].data = getStockDataInQuarters(updateTo, "DEBT_R");
			stabilityChart.update();

			activityChart.data.datasets[0].data=getStockDataInQuarters(updateTo, "ASST_TO");
			activityChart.data.datasets[1].data=getStockDataInQuarters(updateTo, "SA_C_TO");
			activityChart.data.datasets[2].data=getStockDataInQuarters(updateTo, "ST_TO");
			activityChart.update();

			newsCountChart.data.labels = getLabelLast4Weeks(updateTo);
			newsCountChart.data.datasets[0].data=getDataLast4Weeks(updateTo, "news_count");
			discussionCountChart.data.labels = getLabelLast4Weeks(updateTo);
			discussionCountChart.data.datasets[0].data=getDataLast4Weeks(updateTo, "discussion_count");
			newsCountChart.update();
			discussionCountChart.update();
			updateLabel(updateTo);
        }
        $("#stockNameInput").val("");
    });
    }