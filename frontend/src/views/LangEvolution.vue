<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Langage Evolution</h1>
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
          fr
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          en
        </v-btn>
         <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          es
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          ja
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="langEvolutionData.length >0" >
        <line-chart :chartData="langEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" label="Number of tweets"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
//import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'LangEvolution',
  components: {
    LineChart
  },
  data: () => ({
   langEvolutionData: [
      {date: 'Mar 01', total: 65178},
      {date: 'Mar 02', total: 86740},
      {date: 'Mar 03', total: 88191},
      {date: 'Mar 04', total: 92169},
      {date: 'Mar 05', total: 89313},
      {date: 'Mar 06', total: 81284},
      {date: 'Mar 07', total: 79791},
      {date: 'Mar 08', total: 93598},
      {date: 'Mar 09', total: 87268},
      {date: 'Mar 10', total: 67573},
      {date: 'Mar 11', total: 71943},
      {date: 'Mar 12', total: 86737},
      {date: 'Mar 13', total: 75951},
      {date: 'Mar 14', total: 76746},
      {date: 'Mar 15', total: 90599},
      {date: 'Mar 16', total: 105667},
      {date: 'Mar 17', total: 99400},
      {date: 'Mar 18', total: 89919},
      {date: 'Mar 19', total: 93289},
      {date: 'Mar 20', total: 94281},
      {date: 'Mar 21', total: 91340},
    ],
    chartColors: {
      borderColor: "rgba(30, 30, 250, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(43, 43, 231, 0.4)",
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
