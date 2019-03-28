import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ChartDemoComponent } from './chart/chartdemo.component';

const routes: Routes = [
  { path: '', component: ChartDemoComponent },
  { path: 'chartdemo', component: ChartDemoComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
