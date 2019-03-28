import { Component, OnInit } from '@angular/core';

import { Apollo } from 'apollo-angular';
import gql from 'graphql-tag';
import { Subscription } from 'apollo-client/util/Observable';

import * as Highcharts from 'highcharts';
import Drilldown from 'highcharts/modules/drilldown';
Drilldown(Highcharts);
import Exporting from 'highcharts/modules/exporting';
Exporting(Highcharts);


const qry = gql`
query getData($imei: String!){
    get_data(imei: $imei){
      id
      imei
      avgspeed
      windowendtime
    }
  }`;

@Component({
  selector: 'chartdemo',
  templateUrl: 'chartdemo.component.html',
  styleUrls: ['chartdemo.component.css']
})
export class ChartDemoComponent implements OnInit {
  public chart: any;
  public querySubscription: Subscription;
  title = 'UI';
  tableData: any[];
  loading = true;
  error: any;
  selectedImei: string;
  speedData: any[]
  timeData: any[]
  options: object;
  jsonobject: any;
  imeiList: any[] = ['AA001', 'AA002', 'AA003', 'AA004'];


  constructor(private apollo: Apollo) { }

  ngOnInit() {

    this.selectedImei = this.imeiList[0];
    this.getData(this.selectedImei);
  }

  showGraph() {
    this.getData(this.selectedImei);
    console.log("invoked show graph:: ", this.selectedImei);
  }
  getData(selectedVehicle: any) {
    console.log("get messages method");

    this.apollo.watchQuery<any>({
      query: qry,
      variables: {
        imei: selectedVehicle,
      },
    }).refetch().then(result => {
      console.log("cassandra data : " + result);
      this.error = result.errors;
      this.loading = result.loading;
      this.tableData = result.data.get_data;
      this.showBar(this.tableData);
    });

  }

  ngOnDestroy() {
    this.querySubscription.unsubscribe();
  }
  showBar(dataArray: any) {
    this.jsonobject = [];
    dataArray.forEach(element => {
      this.jsonobject.push({ 'name': element.windowendtime, 'y': element.avgspeed })
    });

    this.options = {

      title: { text: "Speed Analysis" },
      chart: {
        type: 'bar',
        zoomType: 'x',
        panning: true,
        panKey: 'shift'
      },
      plotOptions: {
        series: {
          animation: false,

          dataLabels: {
            enabled: true,
            format: '{point.y}'
          }

        },

      },
      xAxis: {
        type: 'category'
      },
      yAxis: {
        title: {
          text: ''
        }
      },
      "series": [
        {
          "name": "Speed",
          "colorByPoint": true,
          "data": this.jsonobject
        }
      ]

    }
    this.querySubscription.unsubscribe();

  }

}

