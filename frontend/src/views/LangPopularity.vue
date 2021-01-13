<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Language Popularity</h1>
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
          top 5
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          top 10
        </v-btn>
        <v-btn class="mr-5"
          rounded
          color="primary"
          @click="wichButton"
          dark
        >
          top 15
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="languagePopularityData.length >0" >
        <horizontal-bar :chartData="languagePopularityData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" label="Number of tweets"></horizontal-bar>
      </div>
      </v-row>
    </v-container>
</template>

<script>
//import axios from 'axios';
import HorizontalBar from '../components/HorizontalBar.vue';

export default {
  name: 'LanguagePopularity',
  components: {
    HorizontalBar,
  },
  data: () => ({
    languagePopularityData: [
      {date: 'eng', total: 21591978},
      {date: 'ja', total: 13395073},
      {date: 'es', total: 6154690},
      {date: 'pt', total: 5282354},
      {date: 'th', total: 4242832},
      {date: 'ar', total: 3545526},
      {date: 'ko', total: 3406221},
      {date: 'in', total: 2383066},
      {date: 'fr', total: 1818890},
      {date: 'tr', total: 1626518},
    ],
    chartColors: {
      backgroundColor: "#885d8f",
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
