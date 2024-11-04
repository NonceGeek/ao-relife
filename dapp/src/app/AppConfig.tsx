export class AppConfig {
  public static siteName = 'Up Up';
  public static secretPassword = 'ploy';

  public static menu = [
    {
      text: 'Index',
      to: '/',
      beta: false,
      loggedIn: false
    },
    {
      text: 'WhitePaper',
      to: 'https://bodhi.wtf/15266',
      beta: false,
      loggedIn: false
    }
  ];
}