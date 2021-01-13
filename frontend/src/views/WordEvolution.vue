<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Word Evolution</h1>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          Confinement
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          Virus
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="wordEvolutionData.length >0" >
        <line-chart :chartData="wordEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" label="confinement"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
//import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'WordEvolution',
  components: {
    LineChart
  },
  data: () => ({
    wordEvolutionData: [
      {date: 'Mar 01', total: 20},
      {date: 'Mar 02', total: 28},
      {date: 'Mar 03', total: 24},
      {date: 'Mar 04', total: 29},
      {date: 'Mar 05', total: 26},
      {date: 'Mar 06', total: 32},
      {date: 'Mar 07', total: 58},
      {date: 'Mar 08', total: 47},
      {date: 'Mar 09', total: 53},
      {date: 'Mar 10', total: 59},
      {date: 'Mar 11', total: 66},
      {date: 'Mar 12', total: 171},
      {date: 'Mar 13', total: 481},
      {date: 'Mar 14', total: 719},
      {date: 'Mar 15', total: 2349},
      {date: 'Mar 16', total: 6647},
      {date: 'Mar 17', total: 6351},
      {date: 'Mar 18', total: 5485},
      {date: 'Mar 19', total: 5909},
      {date: 'Mar 20', total: 6135},
      {date: 'Mar 21', total: 5888},
    ],
    chartColors: {
      borderColor: "rgba(250, 30, 30, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(231, 43, 43, 0.4)",
    },
    chartOptions: {
      responsive: true,
    }
  }),
  methods: {
    wichButton(){
      console.log("bouton");
    }
  },
}
</script>
