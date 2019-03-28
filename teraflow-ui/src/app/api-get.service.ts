import { Injectable } from '@angular/core';
import gql from 'graphql-tag';
import { Subscription } from 'apollo-client/util/Observable';
import { Apollo } from 'apollo-angular';
import { Observable } from 'rxjs';

const qry = gql`
query getData{
    get_data{
      id
      imei
          avgspeed
      windowendtime
    }
  }
      `;

@Injectable({
    providedIn: 'root'
})
export class ApiGetService {
    cassandraData: Observable<any>;
    error: any;
    loading: boolean;
    private querySubscription: Subscription;

    constructor(private apollo: Apollo) {

    }
    public getCassandraData(): Observable<any> {
        this.querySubscription = this.apollo.watchQuery<any>({
            query: qry
        }).valueChanges.subscribe(result => {
            console.log(result);
            this.error = result.errors;
            this.loading = result.loading;
            this.cassandraData = result.data && result.data.get_data;
            console.log(this.cassandraData);
        })
        return this.cassandraData;
    }

}
