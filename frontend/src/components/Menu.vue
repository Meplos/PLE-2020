<template>
  <v-card
    class="mx-auto"
    height="300"
    width="300"
  >
    <v-navigation-drawer
      absolute
      dark
      width="100%"
      permanent
    >
      <v-list>
        <v-list-item
          v-for="([icon, text, path], i) in items"
          :key="i"
          :to="path"
          link
        >
          <v-list-item-icon>
            <v-icon>{{ icon }}</v-icon>
          </v-list-item-icon>

          <v-list-item-content>
            <v-list-item-title>{{ text }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>
  </v-card>
</template>

<script>
import { mdiChartLine, mdiChartGantt, mdiFinance, mdiChartLineStacked, mdiChartAreasplineVariant } from '@mdi/js';

  export default {
    data: () => ({
      items: [
        [mdiChartLine, 'Word Evolution','/wordevo'],
        [mdiFinance, '# Evolution','/hashtagevo'],
        [mdiChartLineStacked, 'Language Evolution','/langevo'],
        [mdiChartAreasplineVariant, 'Language Popularity','/langpop'],
        [mdiChartGantt, 'Locations Popularity','/locpop'],

      ],
    }),
  }
</script>