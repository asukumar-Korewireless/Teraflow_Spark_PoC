import { Component } from '@angular/core';

import { Apollo } from 'apollo-angular';
import gql from 'graphql-tag';
import { Subscription } from 'apollo-client/util/Observable';


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
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {

  public querySubscription: Subscription;
  title = 'UI';
  tableData: any[];
  loading = true;
  error: any;
  selectedImei: string;
  buttonLabel: string = 'Vehicle 1';

  vehicles: any[] = [{ name: 'Vehicle 1', imei: '000013612345678' }, { name: 'Vehicle 2', imei: '000013612345679' }, { name: 'Vehicle 3', imei: '000013612345680' }, { name: 'Vehicle 4', imei: '000013612345681' },
  { name: 'Vehicle 5', imei: '000013612345682' }, { name: 'Vehicle 6', imei: '000013612345683' }];

  imeiList: any[] = ['AA001', 'AA002', 'AA003', 'AA004'];

  constructor(private apollo: Apollo) { }

  ngOnInit() {
    this.selectedImei = this.imeiList[0];
    this.getData(this.selectedImei);
    console.log("after get messages");
  }

  showGraph() {
    this.getData(this.selectedImei);
    console.log("invoked show graph:: ", this.selectedImei);
  }
  getData(selectedVehicle: any) {

    //this.buttonLabel = selectedVehicle.name;
    console.log("get messages method");
    this.querySubscription = this.apollo.watchQuery<any>({
      query: qry,
      variables: {
        imei: selectedVehicle,
      },
    }).valueChanges.subscribe(result => {
      console.log("cassandra data : " + result);
      this.error = result.errors;
      this.loading = result.loading;
      this.tableData = result.data && result.data.get_data;
    });
  }

}
