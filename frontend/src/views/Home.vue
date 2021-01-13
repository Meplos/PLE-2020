<template>
  <v-container fill-height fluid>
    <v-row align="center"
    justify="center">
      <Menu/>
    </v-row>
  </v-container>
</template>

<script>
// @ is an alias to /src
import Menu from '../components/Menu'

export default {
  name: 'Home',
  components: {
    Menu
  }
}
</script>
