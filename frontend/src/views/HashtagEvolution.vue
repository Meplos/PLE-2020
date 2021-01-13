<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Hashtag Evolution</h1>
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
          covid19
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          bts
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="hashtagEvolutionData.length >0" >
        <line-chart :chartData="hashtagEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" label="covid19"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
//import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'HashtagEvolution',
  components: {
    LineChart
  },
  data: () => ({
    hashtagEvolutionData: [
      {date: 'Mar 01', total: 1711},
      {date: 'Mar 02', total: 3504},
      {date: 'Mar 03', total: 2960},
      {date: 'Mar 04', total: 2418},
      {date: 'Mar 05', total: 2790},
      {date: 'Mar 06', total: 2730},
      {date: 'Mar 07', total: 3309},
      {date: 'Mar 08', total: 4217},
      {date: 'Mar 09', total: 4370},
      {date: 'Mar 10', total: 4789},
      {date: 'Mar 11', total: 7344},
      {date: 'Mar 12', total: 10235},
      {date: 'Mar 13', total: 8951},
      {date: 'Mar 14', total: 9799},
      {date: 'Mar 15', total: 8092},
      {date: 'Mar 16', total: 10310},
      {date: 'Mar 17', total: 11629},
      {date: 'Mar 18', total: 12437},
      {date: 'Mar 19', total: 14361},
      {date: 'Mar 20', total: 11548},
      {date: 'Mar 21', total: 10444},
    ],
    chartColors: {
      borderColor: "rgba(30, 250, 30, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(43, 231, 43, 0.4)",
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
